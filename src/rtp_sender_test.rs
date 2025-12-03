#[cfg(test)]
mod tests {
    use crate::media::frame::{AudioFrame, MediaKind};
    use crate::media::track::sample_track;
    use crate::peer_connection::RtpSender;
    use crate::transports::ice::IceSocketWrapper;
    use crate::transports::ice::conn::IceConn;
    use crate::transports::rtp::RtpTransport;
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::net::UdpSocket;
    use tokio::sync::watch;

    #[tokio::test]
    async fn rtp_sender_rewrites_sequence_numbers() {
        // 1. Setup dummy transport
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let socket_wrapper = IceSocketWrapper::Udp(Arc::new(socket));
        let (_tx, rx) = watch::channel(Some(socket_wrapper));

        // Receiver socket to verify packets
        let receiver_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let receiver_addr = receiver_socket.local_addr().unwrap();

        let ice_conn = IceConn::new(rx, receiver_addr);
        let rtp_transport = Arc::new(RtpTransport::new(ice_conn));

        // 2. Create a track and source
        let (source, track, _) = sample_track(MediaKind::Audio, 10);

        // 3. Create RtpSender
        let sender = Arc::new(RtpSender::new(track, 12345));
        sender.set_transport(rtp_transport);

        // 4. Send samples with non-continuous sequence numbers
        let mut buf = [0u8; 1500];

        // Sample 1: Seq 100
        source
            .send_audio(AudioFrame {
                sequence_number: Some(100),
                data: Bytes::from_static(&[1, 2, 3]),
                ..AudioFrame::default()
            })
            .await
            .unwrap();

        // Receive Packet 1
        let (len, _) = receiver_socket.recv_from(&mut buf).await.unwrap();
        let packet1 = crate::rtp::RtpPacket::parse(&buf[..len]).unwrap();
        let seq1 = packet1.header.sequence_number;

        // Sample 2: Seq 200 (Gap in source)
        source
            .send_audio(AudioFrame {
                sequence_number: Some(200),
                data: Bytes::from_static(&[4, 5, 6]),
                ..AudioFrame::default()
            })
            .await
            .unwrap();

        // Receive Packet 2
        let (len, _) = receiver_socket.recv_from(&mut buf).await.unwrap();
        let packet2 = crate::rtp::RtpPacket::parse(&buf[..len]).unwrap();
        let seq2 = packet2.header.sequence_number;

        // Verify continuity
        assert_eq!(
            seq2,
            seq1.wrapping_add(1),
            "Sequence numbers should be continuous despite source gap"
        );
        assert_ne!(seq1, 100, "Sequence number should not match source");
    }
}
