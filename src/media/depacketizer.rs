use crate::media::MediaResult;
use crate::media::frame::{MediaKind, MediaSample, VideoFrame, VideoPixelFormat};
use crate::rtp::RtpPacket;
use bytes::Bytes;
use std::net::SocketAddr;

pub trait Depacketizer: Send + Sync {
    fn push(
        &mut self,
        packet: RtpPacket,
        clock_rate: u32,
        addr: SocketAddr,
        kind: MediaKind,
    ) -> MediaResult<Vec<MediaSample>>;
}

pub struct PassThroughDepacketizer;

impl Depacketizer for PassThroughDepacketizer {
    fn push(
        &mut self,
        packet: RtpPacket,
        clock_rate: u32,
        addr: SocketAddr,
        kind: MediaKind,
    ) -> MediaResult<Vec<MediaSample>> {
        Ok(vec![MediaSample::from_rtp_packet(
            packet, kind, clock_rate, addr,
        )])
    }
}

/// H.264 Depacketizer (RFC 6184)
/// Handles Single NAL Unit, STAP-A, and FU-A.
pub struct H264Depacketizer {
    // Buffer for reassembling FU-A packets
    fua_buffer: Vec<u8>,
    // Expected sequence number for the next FU-A packet
    last_seq: Option<u16>,
    // Timestamp for the current frame being reassembled
    current_timestamp: u32,
}

impl H264Depacketizer {
    pub fn new() -> Self {
        Self {
            fua_buffer: Vec::new(),
            last_seq: None,
            current_timestamp: 0,
        }
    }
}

impl Depacketizer for H264Depacketizer {
    fn push(
        &mut self,
        packet: RtpPacket,
        clock_rate: u32,
        addr: SocketAddr,
        kind: MediaKind,
    ) -> MediaResult<Vec<MediaSample>> {
        if kind == MediaKind::Audio {
            return Ok(vec![MediaSample::from_rtp_packet(
                packet, kind, clock_rate, addr,
            )]);
        }

        let raw_packet = packet.clone();
        let payload = packet.payload;
        if payload.is_empty() {
            // Treat empty payload as a keep-alive/padding frame and pass it through
            return Ok(vec![MediaSample::from_rtp_packet(
                raw_packet, kind, clock_rate, addr,
            )]);
        }

        let header = payload[0];
        let nal_type = header & 0x1F;
        let mut samples = Vec::new();

        let create_video_sample = |data: Bytes, timestamp: u32, is_last: bool, pkt: &RtpPacket| {
            MediaSample::Video(VideoFrame {
                rtp_timestamp: timestamp,
                width: 0,
                height: 0,
                format: VideoPixelFormat::Unspecified,
                rotation_deg: 0,
                is_last_packet: is_last,
                data,
                header_extension: pkt.header.extension.clone(),
                csrcs: pkt.header.csrcs.clone(),
                sequence_number: Some(pkt.header.sequence_number),
                payload_type: Some(pkt.header.payload_type),
                source_addr: Some(addr),
                raw_packet: Some(pkt.clone()),
            })
        };

        match nal_type {
            // STAP-A (Single-Time Aggregation Packet type A)
            24 => {
                let mut offset = 1; // Skip STAP-A header
                let data = Bytes::from(payload);
                let len = data.len();
                let packet_marker = raw_packet.header.marker;

                while offset + 2 < len {
                    let nal_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
                    offset += 2;

                    if offset + nal_len > len {
                        tracing::warn!("STAP-A NAL length exceeds packet size");
                        break;
                    }

                    let nal_data = data.slice(offset..offset + nal_len);
                    offset += nal_len;

                    let is_last = (offset == len) && packet_marker;

                    samples.push(create_video_sample(
                        nal_data,
                        raw_packet.header.timestamp,
                        is_last,
                        &raw_packet,
                    ));
                }
            }
            // FU-A (Fragmentation Unit type A)
            28 => {
                if payload.len() < 2 {
                    return Ok(vec![]);
                }

                let fu_header = payload[1];
                let s_bit = (fu_header & 0x80) != 0;
                let e_bit = (fu_header & 0x40) != 0;
                let original_nal_type = fu_header & 0x1F;

                if s_bit {
                    // Start
                    let nri = header & 0x60;
                    let reconstructed_header = nri | original_nal_type;

                    self.fua_buffer.clear();
                    self.fua_buffer.push(reconstructed_header);
                    self.fua_buffer.extend_from_slice(&payload[2..]);

                    self.current_timestamp = raw_packet.header.timestamp;
                    self.last_seq = Some(raw_packet.header.sequence_number);
                } else {
                    // Continuation or End
                    if let Some(last_seq) = self.last_seq {
                        let expected = last_seq.wrapping_add(1);
                        if raw_packet.header.sequence_number != expected {
                            tracing::warn!(
                                "FU-A Sequence mismatch: expected {}, got {}",
                                expected,
                                raw_packet.header.sequence_number
                            );
                            self.fua_buffer.clear();
                            self.last_seq = None;
                            return Ok(vec![]);
                        }
                    } else {
                        return Ok(vec![]); // Missing start
                    }

                    if raw_packet.header.timestamp != self.current_timestamp {
                        tracing::warn!("FU-A timestamp mismatch inside frame");
                        self.fua_buffer.clear();
                        self.last_seq = None;
                        return Ok(vec![]);
                    }

                    self.fua_buffer.extend_from_slice(&payload[2..]);
                    self.last_seq = Some(raw_packet.header.sequence_number);

                    if e_bit {
                        // End of fragment, emit frame
                        let data = Bytes::from(self.fua_buffer.clone());
                        samples.push(create_video_sample(
                            data,
                            self.current_timestamp,
                            raw_packet.header.marker,
                            &raw_packet,
                        ));

                        self.fua_buffer.clear();
                        self.last_seq = None;
                    }
                }
            }
            // Single NAL unit (1-23)
            1..=23 => {
                let data = Bytes::from(payload);
                samples.push(create_video_sample(
                    data,
                    raw_packet.header.timestamp,
                    raw_packet.header.marker,
                    &raw_packet,
                ));
            }
            // Unknown or unsupported type headers (fallback)
            _ => {
                let data = Bytes::from(payload);
                samples.push(create_video_sample(
                    data,
                    raw_packet.header.timestamp,
                    raw_packet.header.marker,
                    &raw_packet,
                ));
            }
        }

        Ok(samples)
    }
}

pub trait DepacketizerFactory: std::fmt::Debug + Send + Sync {
    fn create(&self, kind: MediaKind) -> Box<dyn Depacketizer>;
}

#[derive(Debug, Default)]
pub struct DefaultDepacketizerFactory;

impl DepacketizerFactory for DefaultDepacketizerFactory {
    fn create(&self, kind: MediaKind) -> Box<dyn Depacketizer> {
        match kind {
            MediaKind::Video => Box::new(H264Depacketizer::new()),
            _ => Box::new(PassThroughDepacketizer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rtp::RtpHeader;
    use std::net::{IpAddr, Ipv4Addr};

    fn create_packet(
        payload: Vec<u8>,
        sequence_number: u16,
        timestamp: u32,
        marker: bool,
    ) -> RtpPacket {
        let mut header = RtpHeader::new(96, sequence_number, timestamp, 12345);
        header.marker = marker;
        RtpPacket::new(header, payload)
    }

    fn dummy_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234)
    }

    #[test]
    fn test_single_nal() {
        let mut depacketizer = H264Depacketizer::new();
        let payload = vec![0x65, 0x01, 0x02, 0x03]; // IDR slice (type 5)
        let packet = create_packet(payload.clone(), 1, 100, true);

        let frames = depacketizer
            .push(packet, 90000, dummy_addr(), MediaKind::Video)
            .unwrap();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            MediaSample::Video(v) => {
                assert_eq!(v.data, Bytes::from(payload));
                assert_eq!(v.rtp_timestamp, 100);
                assert!(v.is_last_packet);
            }
            _ => panic!("Expected Video sample"),
        }
    }

    #[test]
    fn test_stap_a() {
        let mut depacketizer = H264Depacketizer::new();
        // STAP-A header (24)
        let mut payload = vec![24];

        let nal1 = vec![0x67, 0x10]; // SPS
        let len1 = (nal1.len() as u16).to_be_bytes();
        payload.extend_from_slice(&len1);
        payload.extend_from_slice(&nal1);

        let nal2 = vec![0x68, 0x20, 0x30]; // PPS
        let len2 = (nal2.len() as u16).to_be_bytes();
        payload.extend_from_slice(&len2);
        payload.extend_from_slice(&nal2);

        let packet = create_packet(payload, 2, 200, true); // Marker true
        let frames = depacketizer
            .push(packet, 90000, dummy_addr(), MediaKind::Video)
            .unwrap();

        assert_eq!(frames.len(), 2);

        match &frames[0] {
            MediaSample::Video(v) => {
                assert_eq!(v.data, Bytes::from(nal1));
                assert_eq!(v.rtp_timestamp, 200);
                assert_eq!(v.is_last_packet, false); // Not last in packet
            }
            _ => panic!("Expected Video sample"),
        }

        match &frames[1] {
            MediaSample::Video(v) => {
                assert_eq!(v.data, Bytes::from(nal2));
                assert_eq!(v.rtp_timestamp, 200);
                assert_eq!(v.is_last_packet, true); // Last in packet inherits marker
            }
            _ => panic!("Expected Video sample"),
        }
    }

    #[test]
    fn test_fu_a() {
        let mut depacketizer = H264Depacketizer::new();
        let timestamp = 300;

        // Start: FU Indicator 0x7C (Type 28), FU Header 0x85 (S=1, Type=5)
        let packet1 = create_packet(vec![0x7C, 0x85, 0x01, 0x02], 10, timestamp, false);
        let frames1 = depacketizer
            .push(packet1, 90000, dummy_addr(), MediaKind::Video)
            .unwrap();
        assert_eq!(frames1.len(), 0);

        // Middle
        let packet2 = create_packet(vec![0x7C, 0x05, 0x03], 11, timestamp, false);
        let frames2 = depacketizer
            .push(packet2, 90000, dummy_addr(), MediaKind::Video)
            .unwrap();
        assert_eq!(frames2.len(), 0);

        // End: FU Header 0x45 (E=1, Type=5)
        let packet3 = create_packet(vec![0x7C, 0x45, 0x04], 12, timestamp, true);
        let frames3 = depacketizer
            .push(packet3, 90000, dummy_addr(), MediaKind::Video)
            .unwrap();
        assert_eq!(frames3.len(), 1);

        let expected_nal = vec![0x65, 0x01, 0x02, 0x03, 0x04];
        match &frames3[0] {
            MediaSample::Video(v) => {
                assert_eq!(v.data, Bytes::from(expected_nal));
                assert_eq!(v.rtp_timestamp, timestamp);
                assert!(v.is_last_packet);
            }
            _ => panic!("Expected Video sample"),
        }
    }

    #[test]
    fn test_passthrough() {
        let mut depacketizer = PassThroughDepacketizer;
        let payload = vec![0x01, 0x02, 0x03];
        let packet = create_packet(payload.clone(), 1, 100, true);

        let frames = depacketizer
            .push(packet, 48000, dummy_addr(), MediaKind::Audio)
            .unwrap();
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            MediaSample::Audio(a) => {
                assert_eq!(a.data, Bytes::from(payload));
                assert_eq!(a.clock_rate, 48000);
            }
            _ => panic!("Expected Audio sample"),
        }
    }

    #[test]
    fn test_default_factory() {
        let factory = DefaultDepacketizerFactory;

        // Video should produce H264Depacketizer
        // Verify it's not passthrough by sending a partitioned H264 packet (FU-A Start).
        let mut depacketizer = factory.create(MediaKind::Video);
        let timestamp = 12345;
        // FU-A Start: Indicator 0x7C (Avg seq), Header 0x85 (S=1)
        let packet1 = create_packet(vec![0x7C, 0x85, 0x01], 10, timestamp, false);
        let res = depacketizer
            .push(packet1, 90000, dummy_addr(), MediaKind::Video)
            .unwrap();
        assert_eq!(res.len(), 0, "H264 depacketizer should buffer FU-A start");

        // Audio should produce PassThrough
        let mut depacketizer = factory.create(MediaKind::Audio);
        let packet2 = create_packet(vec![0x01, 0x02], 20, timestamp, true);
        let res = depacketizer
            .push(packet2, 48000, dummy_addr(), MediaKind::Audio)
            .unwrap();
        assert_eq!(res.len(), 1, "PassThrough should emit immediately");
    }

    #[test]
    fn test_fu_a_loss() {
        let mut depacketizer = H264Depacketizer::new();
        let timestamp = 44444;

        // Start (Seq 10)
        let packet1 = create_packet(vec![0x7C, 0x85, 0x01], 10, timestamp, false);
        let _ = depacketizer
            .push(packet1, 90000, dummy_addr(), MediaKind::Video)
            .unwrap();

        // Skip Middle (Seq 11 missing)

        // End (Seq 12)
        let packet3 = create_packet(vec![0x7C, 0x45, 0x02], 12, timestamp, true);
        let frames = depacketizer
            .push(packet3, 90000, dummy_addr(), MediaKind::Video)
            .unwrap();

        assert_eq!(
            frames.len(),
            0,
            "Should drop frame if sequence gap detected"
        );
    }
}
