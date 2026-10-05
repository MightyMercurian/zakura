//! Manual capacity evidence. Fixture CPU/queue throughput is not a disk or QUIC benchmark.

use super::*;
use crate::zakura::block_sync::{tests::fake_block_at_height, ZakuraBlockSyncConfig};

#[derive(Debug)]
struct FixtureSource {
    template: Arc<block::Block>,
    available: bool,
}

impl Source for FixtureSource {
    fn read(&self, request: Read) -> BoxFuture<'static, Result<ReadResult, crate::BoxError>> {
        let template = self.template.clone();
        let available = self.available;
        Box::pin(async move {
            Ok(tokio::task::spawn_blocking(move || {
                let blocks = if request.lease.try_start() && available {
                    (0..request.count)
                        .map(|offset| {
                            let height = block::Height(request.start_height.0 + offset);
                            (height, fake_block_at_height(&template, height))
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                ReadResult {
                    blocks,
                    lease: request.lease,
                }
            })
            .await?)
        })
    }
}

#[tokio::test]
#[ignore = "manual capacity evidence; requires ZAKURA_CAPACITY_PROBE=1"]
#[allow(clippy::print_stderr)]
async fn serving_capacity_candidates() {
    if std::env::var_os("ZAKURA_CAPACITY_PROBE").is_none() {
        return;
    }
    let template: Arc<block::Block> =
        Arc::new(BLOCK_MAINNET_1_BYTES.zcash_deserialize_into().unwrap());
    for window in [32, 128, 512, 32_000] {
        let config = ZakuraBlockSyncConfig {
            max_inflight_requests: window,
            ..Default::default()
        };
        measure_waiting_getblocks_capacity(1, &config);
        for (shape, available, count, total) in [
            ("unavailable", false, 1, 2048),
            ("one_small_block", true, 1, 2048),
            ("configured_128_block_range", true, 128, 128),
        ] {
            let config = ZakuraBlockSyncConfig {
                max_blocks_per_response: count,
                ..config.clone()
            };
            let source = Arc::new(FixtureSource {
                template: template.clone(),
                available,
            });
            let serving = super::super::session::Serving::new(source, &config);
            let cancel = CancellationToken::new();
            let peer = ZakuraPeerId::new(vec![17; 32]).unwrap();
            let (send, mut output) = framed_channel(64);
            let mut session = serving.session(&peer, send, cancel.clone(), cancel.clone());
            let (mut issued, mut completed, mut bodies, mut wire_bytes) =
                (0_u32, 0_u32, 0_u32, 0_u64);
            let started = std::time::Instant::now();
            while completed < total {
                while issued < total && issued - completed < window {
                    session
                        .admit(block::Height(issued * count + 1), count)
                        .unwrap();
                    issued += 1;
                }
                let frame = tokio::time::timeout(Duration::from_secs(30), output.recv())
                    .await
                    .unwrap()
                    .unwrap();
                wire_bytes +=
                    u64::try_from(frame.payload.len() + crate::zakura::FRAME_HEADER_BYTES).unwrap();
                if frame.message_type == 3 {
                    bodies += 1;
                }
                if matches!(frame.message_type, 4 | 5) {
                    completed += 1;
                }
            }
            assert_eq!(bodies, if available { total * count } else { 0 });
            let elapsed = started.elapsed();
            let bytes_per_response = wire_bytes / u64::from(total);
            let bdp_entries = (2 * crate::zakura::regulation::sizing::bandwidth_delay_bytes())
                .div_ceil(bytes_per_response);
            eprintln!("fixture shape={shape} window={window} responses={total} seconds={:.6} wire_bytes={wire_bytes} bytes_per_response={bytes_per_response} requests_per_second={:.1} entries_for_twice_target_bdp={bdp_entries}", elapsed.as_secs_f64(), f64::from(total) / elapsed.as_secs_f64());
            cancel.cancel();
        }
    }
}
