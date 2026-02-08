use super::*;
use crate::transports::PacketReceiver;
use crate::{IceServer, IceTransportPolicy, RtcConfiguration};
use ::turn::{
    auth::{AuthHandler, generate_auth_key},
    relay::relay_static::RelayAddressGeneratorStatic,
    server::{
        Server,
        config::{ConnConfig, ServerConfig},
    },
};
use anyhow::Result;
use bytes::Bytes;
use futures::FutureExt;
use tokio::sync::broadcast;
use tokio::time::{Duration, timeout};
// use webrtc_util::vnet::net::Net;
type TurnResult<T> = std::result::Result<T, ::turn::Error>;

#[test]
fn parse_turn_uri() {
    let uri = IceServerUri::parse("turn:example.com:3478?transport=tcp").unwrap();
    assert_eq!(uri.host, "example.com");
    assert_eq!(uri.port, 3478);
    assert_eq!(uri.transport, IceTransportProtocol::Tcp);
    assert_eq!(uri.kind, IceUriKind::Turn);
}

#[tokio::test]
async fn builder_starts_gathering() {
    let (transport, runner) = IceTransportBuilder::new(RtcConfiguration::default()).build();
    tokio::spawn(runner);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(
        transport.gather_state(),
        IceGathererState::Complete
    ));
}

#[tokio::test]
async fn stun_probe_yields_server_reflexive_candidate() -> Result<()> {
    let mut turn_server = TestTurnServer::start().await?;
    let mut config = RtcConfiguration::default();
    config
        .ice_servers
        .push(IceServer::new(vec![turn_server.stun_url()]));
    let (tx, _) = broadcast::channel(100);
    let (socket_tx, _) = tokio::sync::mpsc::unbounded_channel();
    let gatherer = IceGatherer::new(config, tx, socket_tx);
    gatherer.gather().await?;
    let candidates = gatherer.local_candidates();
    assert!(
        candidates
            .iter()
            .any(|c| matches!(c.typ, IceCandidateType::ServerReflexive))
    );
    turn_server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn turn_probe_yields_relay_candidate() -> Result<()> {
    let mut turn_server = TestTurnServer::start().await?;
    let mut config = RtcConfiguration::default();
    config.ice_servers.push(
        IceServer::new(vec![turn_server.turn_url()]).with_credential(TEST_USERNAME, TEST_PASSWORD),
    );
    let (tx, _) = broadcast::channel(100);
    let (socket_tx, _) = tokio::sync::mpsc::unbounded_channel();
    let gatherer = IceGatherer::new(config, tx, socket_tx);
    gatherer.gather().await?;
    let candidates = gatherer.local_candidates();
    assert!(
        candidates
            .iter()
            .any(|c| matches!(c.typ, IceCandidateType::Relay))
    );
    turn_server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn policy_relay_only_gathers_relay_candidates() -> Result<()> {
    let mut turn_server = TestTurnServer::start().await?;
    let mut config = RtcConfiguration::default();
    config.ice_transport_policy = IceTransportPolicy::Relay;
    config.ice_servers.push(
        IceServer::new(vec![turn_server.turn_url()]).with_credential(TEST_USERNAME, TEST_PASSWORD),
    );

    // Add a STUN server too, to verify it is ignored
    config
        .ice_servers
        .push(IceServer::new(vec![turn_server.stun_url()]));

    let (tx, _) = broadcast::channel(100);
    let (socket_tx, _) = tokio::sync::mpsc::unbounded_channel();
    let gatherer = IceGatherer::new(config, tx, socket_tx);
    gatherer.gather().await?;
    let candidates = gatherer.local_candidates();

    assert!(!candidates.is_empty());
    for c in candidates {
        assert_eq!(
            c.typ,
            IceCandidateType::Relay,
            "Found non-relay candidate: {:?}",
            c
        );
    }

    turn_server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn turn_client_can_create_permission() -> Result<()> {
    let mut turn_server = TestTurnServer::start().await?;
    let uri = IceServerUri::parse(&turn_server.turn_url())?;
    let server =
        IceServer::new(vec![turn_server.turn_url()]).with_credential(TEST_USERNAME, TEST_PASSWORD);
    let client = TurnClient::connect(&uri, false).await?;
    let creds = TurnCredentials::from_server(&server)?;
    client.allocate(creds).await?;
    let peer: SocketAddr = "127.0.0.1:5000".parse().unwrap();
    client.create_permission(peer).await?;
    turn_server.stop().await?;
    Ok(())
}

#[test]
fn candidate_pair_priority_calculation() {
    let local = IceCandidate::host("127.0.0.1:1000".parse().unwrap(), 1);
    let remote = IceCandidate::host("127.0.0.1:2000".parse().unwrap(), 1);
    let pair = IceCandidatePair::new(local.clone(), remote.clone());

    // G = local.priority, D = remote.priority
    // Since both are host/1, priorities should be equal.
    let p1 = pair.priority(IceRole::Controlling);
    let p2 = pair.priority(IceRole::Controlled);

    assert_eq!(p1, p2);

    // Test with different priorities
    let local_relay = IceCandidate::relay("127.0.0.1:1000".parse().unwrap(), 1, "udp");
    let pair2 = IceCandidatePair::new(local_relay, remote);

    // Relay has lower priority than Host.
    // Controlling: G (relay) < D (host)
    // Controlled: D (relay) < G (host)

    let prio_controlling = pair2.priority(IceRole::Controlling);
    let prio_controlled = pair2.priority(IceRole::Controlled);

    // Formula: 2^32*MIN(G,D) + 2*MAX(G,D) + (G>D?1:0)
    // Since priorities are different, the MIN term dominates.
    // In both roles, the set of {G, D} is the same, so MIN(G,D) and MAX(G,D) are same.
    // The only difference is the tie breaker (G>D?1:0).

    // If G < D (Controlling case here): term is 0.
    // If G > D (Controlled case here, since G becomes host): term is 1.

    assert!(prio_controlled > prio_controlling);
    assert_eq!(prio_controlled - prio_controlling, 1);
}

#[tokio::test]
async fn turn_connection_relay_to_host() -> Result<()> {
    let mut turn_server = TestTurnServer::start().await?;

    // Agent 1: Relay only
    let mut config1 = RtcConfiguration::default();
    config1.ice_transport_policy = IceTransportPolicy::Relay;
    config1.ice_servers.push(
        IceServer::new(vec![turn_server.turn_url()]).with_credential(TEST_USERNAME, TEST_PASSWORD),
    );
    let (transport1, runner1) = IceTransportBuilder::new(config1)
        .role(IceRole::Controlling)
        .build();
    tokio::spawn(runner1);

    // Agent 2: Host only
    let config2 = RtcConfiguration::default();
    let (transport2, runner2) = IceTransportBuilder::new(config2)
        .role(IceRole::Controlled)
        .build();
    tokio::spawn(runner2);

    // Exchange candidates
    let t1 = transport1.clone();
    let t2 = transport2.clone();

    let mut rx1 = t1.subscribe_candidates();
    let mut rx2 = t2.subscribe_candidates();

    // Add existing candidates
    for c in t1.local_candidates() {
        t2.add_remote_candidate(c);
    }
    for c in t2.local_candidates() {
        t1.add_remote_candidate(c);
    }

    tokio::spawn(async move {
        while let Ok(c) = rx1.recv().await {
            t2.add_remote_candidate(c);
        }
    });
    tokio::spawn(async move {
        while let Ok(c) = rx2.recv().await {
            t1.add_remote_candidate(c);
        }
    });

    // Wait for connection
    let state1 = transport1.subscribe_state();
    let state2 = transport2.subscribe_state();

    // Start
    transport1.start(transport2.local_parameters())?;
    transport2.start(transport1.local_parameters())?;

    // Wait for Connected
    let wait_connected = |mut state: watch::Receiver<IceTransportState>, name: &'static str| async move {
        loop {
            let s = *state.borrow_and_update();
            if s == IceTransportState::Connected {
                break;
            }
            if s == IceTransportState::Failed {
                panic!("Transport {} failed", name);
            }
            if state.changed().await.is_err() {
                panic!("Transport {} state channel closed", name);
            }
        }
    };

    tokio::try_join!(
        timeout(Duration::from_secs(10), wait_connected(state1, "1")),
        timeout(Duration::from_secs(10), wait_connected(state2, "2"))
    )
    .expect("Timed out waiting for connection");

    // Verify selected pair on transport 1 is Relay
    let pair1 = transport1.get_selected_pair().await.unwrap();
    assert_eq!(pair1.local.typ, IceCandidateType::Relay);

    // Send data
    let (tx1, mut rx1_data) = tokio::sync::mpsc::channel(10);
    let (tx2, mut rx2_data) = tokio::sync::mpsc::channel(10);

    struct TestReceiver(tokio::sync::mpsc::Sender<Bytes>);
    #[async_trait::async_trait]
    impl PacketReceiver for TestReceiver {
        async fn receive(&self, packet: Bytes, _addr: SocketAddr) {
            let _ = self.0.send(packet).await;
        }
    }

    transport1
        .set_data_receiver(Arc::new(TestReceiver(tx1)))
        .await;
    transport2
        .set_data_receiver(Arc::new(TestReceiver(tx2)))
        .await;

    let socket1 = transport1.get_selected_socket().await.unwrap();
    let pair1 = transport1.get_selected_pair().await.unwrap();

    let data = Bytes::from_static(b"hello from 1");
    socket1.send_to(&data, pair1.remote.address).await?;

    let received = timeout(Duration::from_secs(5), rx2_data.recv())
        .await?
        .unwrap();
    assert_eq!(received, data);

    // Send data back
    let socket2 = transport2.get_selected_socket().await.unwrap();
    let pair2 = transport2.get_selected_pair().await.unwrap();
    let data2 = Bytes::from_static(b"hello from 2");
    socket2.send_to(&data2, pair2.remote.address).await?;

    let received2 = timeout(Duration::from_secs(5), rx1_data.recv())
        .await?
        .unwrap();
    assert_eq!(received2, data2);

    turn_server.stop().await?;
    Ok(())
}
#[tokio::test]
async fn test_ice_connection_timeout() -> Result<()> {
    let mut config = RtcConfiguration::default();
    config.ice_connection_timeout = Duration::from_millis(100);

    let (transport, runner) = IceTransportBuilder::new(config).build();
    tokio::spawn(runner);

    // Set state to Connected to trigger keepalive tick logic
    transport
        .inner
        .state
        .send(IceTransportState::Connected)
        .unwrap();

    // Wait for more than 1 second (interval is 1s)
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // Should be Failed now
    assert_eq!(transport.state(), IceTransportState::Failed);

    Ok(())
}
const TEST_USERNAME: &str = "test";
const TEST_PASSWORD: &str = "test";
const TEST_REALM: &str = ".turn";

struct TestTurnServer {
    server: Option<Server>,
    addr: SocketAddr,
}

impl TestTurnServer {
    async fn start() -> Result<Self> {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        let addr = socket.local_addr()?;
        let conn = Arc::new(socket);
        let relay_addr_generator = Box::new(RelayAddressGeneratorStatic {
            relay_address: addr.ip(),
            address: "0.0.0.0".to_string(),
            net: Arc::new(webrtc_util::vnet::net::Net::new(None)),
        });
        let auth_handler = Arc::new(StaticAuthHandler::new(
            TEST_USERNAME.to_string(),
            TEST_PASSWORD.to_string(),
        ));
        let config = ServerConfig {
            conn_configs: vec![ConnConfig {
                conn,
                relay_addr_generator,
            }],
            realm: TEST_REALM.to_string(),
            auth_handler,
            channel_bind_timeout: Duration::from_secs(600),
            alloc_close_notify: None,
        };
        let server = Server::new(config).await?;
        Ok(Self {
            server: Some(server),
            addr,
        })
    }

    fn stun_url(&self) -> String {
        format!("stun:{}", self.addr)
    }

    fn turn_url(&self) -> String {
        format!("turn:{}", self.addr)
    }

    async fn stop(&mut self) -> Result<()> {
        if let Some(server) = self.server.take() {
            server.close().await?;
        }
        Ok(())
    }
}

struct StaticAuthHandler {
    username: String,
    password: String,
}

impl StaticAuthHandler {
    fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
}

impl AuthHandler for StaticAuthHandler {
    fn auth_handle(
        &self,
        username: &str,
        realm: &str,
        _src_addr: SocketAddr,
    ) -> TurnResult<Vec<u8>> {
        if username != self.username {
            return Err(::turn::Error::ErrNoSuchUser);
        }
        Ok(generate_auth_key(username, realm, &self.password))
    }
}

#[test]
fn ice_candidate_foundation_compliance() {
    let addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
    let host = IceCandidate::host(addr, 1);

    // Check foundation format (should be alphanumeric, no colons)
    // The previous implementation used "host:127.0.0.1" which contained ':'
    assert!(!host.foundation.contains(':'));
    assert!(host.foundation.chars().all(|c| c.is_ascii_alphanumeric()));

    // Check SDP output
    let sdp = host.to_sdp();
    assert!(sdp.contains(" typ host"));
    // Should verify it starts with foundation
    let parts: Vec<&str> = sdp.split_whitespace().collect();
    let foundation = parts[0];
    assert_eq!(foundation, host.foundation);

    // Check srflx
    let mapped: SocketAddr = "1.2.3.4:5000".parse().unwrap();
    let srflx = IceCandidate::server_reflexive(addr, mapped, 1);
    assert!(!srflx.foundation.contains(':'));
    assert!(srflx.foundation.chars().all(|c| c.is_ascii_alphanumeric()));

    // Ensure foundation is same for same type/base
    let srflx2 = IceCandidate::server_reflexive(addr, "1.2.3.5:6000".parse().unwrap(), 1);
    assert_eq!(srflx.foundation, srflx2.foundation);

    // Ensure foundation is different for different base
    let addr2: SocketAddr = "192.168.0.1:5000".parse().unwrap();
    let srflx3 = IceCandidate::server_reflexive(addr2, mapped, 1);
    assert_ne!(srflx.foundation, srflx3.foundation);

    // Check relay
    let relay = IceCandidate::relay(mapped, 1, "udp");
    assert!(!relay.foundation.contains(':'));

    // Check that host and srflx have different foundations even if same address (though unlikely in practice for base vs mapped)
    // Actually foundation computation uses type.
    let host_same_addr = IceCandidate::host(addr, 1);
    let srflx_same_base = IceCandidate::server_reflexive(addr, mapped, 1);
    assert_ne!(host_same_addr.foundation, srflx_same_base.foundation);
}

#[tokio::test]
async fn test_ice_lite_stun_response() -> Result<()> {
    use crate::TransportMode;

    // Create ICE-lite transport (RTP mode)
    let mut config = RtcConfiguration::default();
    config.transport_mode = TransportMode::Rtp;
    config.enable_ice_lite = true;
    config.bind_ip = Some("127.0.0.1".to_string());

    let (ice_lite, runner) = IceTransport::new(config);
    tokio::spawn(runner);

    // Set up for RTP mode - bind socket via setup_direct_rtp_offer
    let local_addr = ice_lite.setup_direct_rtp_offer().await?;

    // Get ICE credentials for authentication
    let _local_params = ice_lite.local_parameters();

    // Simulate remote ICE agent with credentials
    let remote_params = IceParameters::new("remote_ufrag", "remote_pwd_12345");
    ice_lite.set_remote_parameters(remote_params.clone());
    ice_lite.set_role(IceRole::Controlled);

    // Create a socket to act as the full-ICE remote agent
    let remote_socket = UdpSocket::bind("127.0.0.1:0").await?;
    let remote_addr = remote_socket.local_addr()?;

    // Craft STUN binding request - try without authentication first
    let tx_id = crate::transports::ice::stun::random_bytes::<12>();
    let binding_request = StunMessage::binding_request(tx_id, Some("ice-lite-test"));

    // Encode without message integrity for basic connectivity
    let request_bytes = binding_request.encode(None, false)?;

    println!(
        "Sending STUN Binding Request from {} to ICE-lite agent at {}",
        remote_addr, local_addr
    );

    // Send STUN binding request to the ICE-lite transport
    remote_socket.send_to(&request_bytes, local_addr).await?;

    // Wait for STUN binding response on the remote socket
    let mut buf = [0u8; 1500];
    let (len, response_from) =
        tokio::time::timeout(Duration::from_secs(5), remote_socket.recv_from(&mut buf))
            .await
            .map_err(|_| anyhow::anyhow!("Should receive STUN response within 5 seconds"))?
            .map_err(|e| anyhow::anyhow!("Socket recv error: {}", e))?;

    println!(
        "Received STUN response from {}, {} bytes",
        response_from, len
    );

    // Verify the response is from the ICE-lite agent
    assert_eq!(
        response_from, local_addr,
        "Response should come from ICE-lite local address"
    );

    // Decode and verify STUN binding success response
    let decoded_response = StunMessage::decode(&buf[..len])?;
    assert_eq!(
        decoded_response.class,
        crate::transports::ice::stun::StunClass::SuccessResponse
    );
    assert_eq!(
        decoded_response.method,
        crate::transports::ice::stun::StunMethod::Binding
    );
    assert_eq!(
        decoded_response.transaction_id, tx_id,
        "Transaction ID should match request"
    );

    // Verify XOR-MAPPED-ADDRESS attribute (should reflect the requester's address)
    assert!(
        decoded_response.xor_mapped_address.is_some(),
        "STUN response should contain XOR-MAPPED-ADDRESS"
    );

    let mapped_addr = decoded_response.xor_mapped_address.unwrap();
    assert_eq!(
        mapped_addr, remote_addr,
        "XOR-MAPPED-ADDRESS should reflect remote agent's address"
    );

    println!("✓ ICE-lite correctly responded to STUN binding request");
    println!("✓ Response contains correct transaction ID and XOR-MAPPED-ADDRESS");

    // Verify that the remote address was added as a peer reflexive candidate
    let candidates = ice_lite.remote_candidates();
    let prflx_candidates: Vec<_> = candidates
        .iter()
        .filter(|c| c.typ == IceCandidateType::PeerReflexive && c.address == remote_addr)
        .collect();

    assert!(
        !prflx_candidates.is_empty(),
        "Remote address should be added as peer-reflexive candidate"
    );
    println!(
        "✓ Peer-reflexive candidate discovered for remote address {}",
        remote_addr
    );

    Ok(())
}

#[tokio::test]
async fn test_ice_lite_connectivity_establishment() -> Result<()> {
    use crate::TransportMode;

    // Set up ICE-lite agent
    let mut lite_config = RtcConfiguration::default();
    lite_config.transport_mode = TransportMode::Rtp;
    lite_config.enable_ice_lite = true;
    lite_config.bind_ip = Some("127.0.0.1".to_string());

    let (ice_lite, lite_runner) = IceTransport::new(lite_config);
    tokio::spawn(lite_runner);

    // Set up full-ICE agent
    let full_config = RtcConfiguration::default();
    let (ice_full, full_runner) = IceTransportBuilder::new(full_config)
        .role(IceRole::Controlling)
        .build();
    tokio::spawn(full_runner);

    // ICE-lite sets up direct RTP socket
    let _lite_addr = ice_lite.setup_direct_rtp_offer().await?;

    // Exchange ICE parameters
    let lite_params = ice_lite.local_parameters();
    let full_params = ice_full.local_parameters();

    ice_lite.set_remote_parameters(full_params.clone());
    ice_lite.set_role(IceRole::Controlled);

    // Add ICE-lite candidate to full agent
    let lite_candidates = ice_lite.local_candidates();
    assert!(
        !lite_candidates.is_empty(),
        "ICE-lite should have local candidates"
    );

    for candidate in lite_candidates {
        ice_full.add_remote_candidate(candidate);
    }

    // Start full ICE agent to trigger candidate gathering
    ice_full.start(lite_params.clone())?;

    // Wait a bit for candidate gathering
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Complete ICE-lite connection with full agent's candidate
    let full_candidates = ice_full.local_candidates();
    let full_host_candidate = full_candidates
        .iter()
        .find(|c| c.typ == IceCandidateType::Host)
        .expect("Full ICE agent should have host candidate")
        .clone();

    ice_lite.complete_direct_rtp(full_host_candidate.address);
    ice_lite.add_remote_candidate(full_host_candidate);

    // Wait for both sides to be connected with simpler wait logic
    let lite_state = ice_lite.subscribe_state();
    let full_state = ice_full.subscribe_state();

    async fn wait_connected(
        mut state: watch::Receiver<IceTransportState>,
        name: &str,
    ) -> Result<()> {
        for _ in 0..50 {
            // 5 second timeout with 100ms intervals
            let current_state = *state.borrow();
            if current_state == IceTransportState::Connected {
                println!("{} transport connected", name);
                return Ok(());
            }
            if current_state == IceTransportState::Failed {
                return Err(anyhow::anyhow!("{} transport failed", name));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = state.changed().now_or_never();
        }
        Err(anyhow::anyhow!(
            "{} transport did not connect within timeout",
            name
        ))
    }

    tokio::try_join!(
        wait_connected(lite_state, "ICE-lite"),
        wait_connected(full_state, "Full ICE")
    )?;

    // Verify selected pairs
    let lite_pair = ice_lite.get_selected_pair().await.unwrap();
    let full_pair = ice_full.get_selected_pair().await.unwrap();

    println!(
        "ICE-lite selected pair: {} -> {}",
        lite_pair.local.address, lite_pair.remote.address
    );
    println!(
        "Full ICE selected pair: {} -> {}",
        full_pair.local.address, full_pair.remote.address
    );

    // Verify data can flow in both directions
    let (lite_tx, mut lite_rx) = tokio::sync::mpsc::channel(10);
    let (full_tx, mut full_rx) = tokio::sync::mpsc::channel(10);

    struct DataReceiver(tokio::sync::mpsc::Sender<Bytes>);

    #[async_trait::async_trait]
    impl PacketReceiver for DataReceiver {
        async fn receive(&self, packet: Bytes, _addr: SocketAddr) {
            // Filter out STUN packets, only forward data
            if !packet.is_empty() && packet[0] >= 2 {
                let _ = self.0.send(packet).await;
            }
        }
    }

    ice_lite
        .set_data_receiver(Arc::new(DataReceiver(lite_tx)))
        .await;
    ice_full
        .set_data_receiver(Arc::new(DataReceiver(full_tx)))
        .await;

    // Send data from full agent to ICE-lite
    let full_socket = ice_full.get_selected_socket().await.unwrap();
    let test_data = Bytes::from_static(b"Hello from full ICE agent");
    full_socket
        .send_to(&test_data, lite_pair.local.address)
        .await?;

    let received_by_lite = timeout(Duration::from_secs(2), lite_rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("ICE-lite did not receive data"))?;
    assert_eq!(received_by_lite, test_data);

    // Send data from ICE-lite to full agent
    let lite_socket = ice_lite.get_selected_socket().await.unwrap();
    let response_data = Bytes::from_static(b"Hello from ICE-lite agent");
    lite_socket
        .send_to(&response_data, full_pair.local.address)
        .await?;

    let received_by_full = timeout(Duration::from_secs(2), full_rx.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Full ICE agent did not receive data"))?;
    assert_eq!(received_by_full, response_data);

    println!("✓ ICE-lite successfully established connectivity with full ICE agent");
    println!("✓ Bidirectional data flow verified");

    Ok(())
}
