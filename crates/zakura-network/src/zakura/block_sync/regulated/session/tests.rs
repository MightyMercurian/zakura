use super::*;
use crate::zakura::{
    block_sync::regulated::serving::{Read, ReadResult},
    framed_channel,
};
use futures::future::BoxFuture;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Duration,
};
use tokio::sync::Semaphore;

/// Gates real serving reads while leaving response production and cleanup unchanged.
#[derive(Debug, Default)]
struct ControlledSource {
    gates: Mutex<BTreeMap<Height, Arc<Semaphore>>>,
    default_gate: Option<Arc<Semaphore>>,
    started: AtomicUsize,
}

impl Source for ControlledSource {
    fn read(&self, request: Read) -> BoxFuture<'static, Result<ReadResult, crate::BoxError>> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let gate = self
            .gates
            .lock()
            .unwrap()
            .get(&request.start_height)
            .cloned()
            .or_else(|| self.default_gate.clone());
        Box::pin(async move {
            if request.lease.try_start() {
                if let Some(gate) = gate {
                    gate.acquire().await?.forget();
                }
            }
            Ok(ReadResult {
                blocks: Vec::new(),
                lease: request.lease,
            })
        })
    }
}

/// Wait for the production completion drain to release a range.
async fn wait_completed(session: &mut ServingSession, start: Height) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            session.remove_completed();
            if !session.ranges.contains_key(&start) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

/// Later responses can finish behind a blocked writer without releasing a replacement twice.
#[tokio::test]
async fn completed_ranges_are_reusable_before_earlier_output_drains() {
    let source = Arc::new(ControlledSource::default());
    let first_gate = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(1), first_gate.clone());
    let serving = Serving::new(source.clone(), &ZakuraBlockSyncConfig::default());
    let cancel = CancellationToken::new();
    let (send, mut output) = framed_channel(4);
    let mut session = serving.session(
        &ZakuraPeerId::new(vec![61; 32]).unwrap(),
        send,
        cancel.clone(),
        CancellationToken::new(),
        Default::default(),
    );
    session.admit(Height(1), 1).unwrap();
    session.admit(Height(2), 1).unwrap();
    wait_completed(&mut session, Height(2)).await;
    assert!(session.ranges.contains_key(&Height(1)));
    assert!(session.admit(Height(1), 1).is_err());
    let next_gate = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(2), next_gate.clone());
    session.admit(Height(2), 1).unwrap();
    first_gate.add_permits(1);
    for height in [1, 2] {
        assert!(matches!(super::super::tests::receive(&mut output).await,
            super::super::wire::Message::RangeUnavailable(range) if range.start == Height(height)));
    }
    session.remove_completed();
    assert!(
        session.ranges.contains_key(&Height(2)),
        "old completion cannot release the replacement"
    );
    assert!(session.admit(Height(2), 1).is_err());
    next_gate.add_permits(1);
    super::super::tests::receive(&mut output).await;
    wait_completed(&mut session, Height(2)).await;
    assert!(session.ranges.is_empty());
    cancel.cancel();
}

/// Finishing a retired session's read cannot remove a range from the replacement session.
#[tokio::test]
async fn cancelled_sessions_do_not_complete_replacement_ranges() {
    let source = Arc::new(ControlledSource::default());
    let old_gate = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(1), old_gate.clone());
    let serving = Serving::new(source.clone(), &ZakuraBlockSyncConfig::default());
    let peer = ZakuraPeerId::new(vec![62; 32]).unwrap();
    let old_cancel = CancellationToken::new();
    let (send, old_output) = framed_channel(4);
    let mut old = serving.session(
        &peer,
        send,
        old_cancel.clone(),
        CancellationToken::new(),
        Default::default(),
    );
    old.admit(Height(1), 1).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while source.started.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    old_cancel.cancel();
    drop((old, old_output));
    let new_gate = Arc::new(Semaphore::new(0));
    source
        .gates
        .lock()
        .unwrap()
        .insert(Height(1), new_gate.clone());
    let cancel = CancellationToken::new();
    let (send, mut output) = framed_channel(4);
    let mut new = serving.session(
        &peer,
        send,
        cancel.clone(),
        CancellationToken::new(),
        Default::default(),
    );
    new.admit(Height(1), 1).unwrap();
    old_gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while source.started.load(Ordering::SeqCst) < 2
            || serving.capacity.node_execution_held() != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    new.remove_completed();
    assert!(new.ranges.contains_key(&Height(1)));
    assert!(new.admit(Height(1), 1).is_err());
    new_gate.add_permits(1);
    super::super::tests::receive(&mut output).await;
    wait_completed(&mut new, Height(1)).await;
    cancel.cancel();
}

/// Exercise the production lifecycle under an external process peak-memory sampler.
/// The thread-local allocator cannot measure ownership moved to encoding workers.
#[tokio::test]
#[ignore = "manual lifecycle measurement; requires ZAKURA_CAPACITY_PROBE=1"]
#[allow(clippy::print_stderr)]
async fn compact_session_lifecycle_peak() {
    if std::env::var_os("ZAKURA_CAPACITY_PROBE").is_none() {
        return;
    }
    let gate = Arc::new(Semaphore::new(0));
    let source = Arc::new(ControlledSource {
        default_gate: Some(gate.clone()),
        ..Default::default()
    });
    let serving = Serving::new(source, &ZakuraBlockSyncConfig::default());
    let peer = ZakuraPeerId::new(vec![63; 32]).unwrap();
    let count = ZakuraBlockSyncConfig::default().advertised_max_inflight_requests() * 2;
    tokio::time::timeout(Duration::from_secs(60), async {
        let cancel = CancellationToken::new();
        let (send, mut output) = framed_channel(4);
        let mut session = serving.session(
            &peer,
            send,
            cancel.clone(),
            CancellationToken::new(),
            Default::default(),
        );
        for height in 1..=count {
            session.admit(Height(height), 1).unwrap();
        }
        gate.add_permits(512);
        for _ in 0..512 {
            let super::super::wire::Message::RangeUnavailable(range) =
                super::super::tests::receive(&mut output).await
            else {
                panic!("empty reads end unavailable");
            };
            session.admit(range.start, 1).unwrap();
        }
        cancel.cancel();
        drop((session, output));
        // Admit a full replacement before the old dispatch has dropped its queued jobs.
        let cancel = CancellationToken::new();
        let (send, output) = framed_channel(4);
        let mut replacement = serving.session(
            &peer,
            send,
            cancel.clone(),
            CancellationToken::new(),
            Default::default(),
        );
        for height in 1..=count {
            replacement.admit(Height(height), 1).unwrap();
        }
        assert_eq!(replacement.ranges.len(), usize::try_from(count).unwrap());
        cancel.cancel();
        drop((replacement, output));
        gate.add_permits(128);
        // Let cancelled dispatchers drop queued jobs, and wait for active storage cleanup.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        while serving.capacity.node_execution_held() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    eprintln!("compact session lifecycle count={count}, refilled=512, replacement={count}, execution_released=true");
}
