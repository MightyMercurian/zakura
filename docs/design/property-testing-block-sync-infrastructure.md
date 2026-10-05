# GetBlocks regulation tests

> **Status: first draft.** This plan covers regulated block-sync version 3 and version 2 compatibility under the
> [testing design](property-testing.md). The
> [specification](../specs/peer-message-regulation.md) defines behavior.

## Scope

Exercise GetBlocks through its Block responses and one BlocksDone or RangeUnavailable terminal.
Use production framing, decode, reservations, serving, and cleanup. Add only the seams needed to
control capacity, storage completion, and response delivery.

This work introduces no Work bucket, refund, mandatory Delay verdict, or serving query-result timer.
It does not require a generic framework or exhaustive explorer. Header subscriptions have separate
tests. Version 3 retains height-range correlation and forbids overlapping live ranges per connection.
Version 2 keeps the legacy behavior for peers that have not negotiated regulation.

## Existing infrastructure

| Capability | Existing path | Use |
| --- | --- | --- |
| Production in-memory peers | [SyntheticBlockSyncPeers](../../crates/zakura-network/src/zakura/testkit/block_sync_peer.rs) | Real stream-6 encoding, decoding, and peer routines |
| Multi-peer configuration | [Scenario and PeerSpec](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/scenario.rs) | Peer, timing, range, queue, and corpus helpers |
| Adversarial behavior | [Synthetic serve loop](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/peer.rs) | Withhold, reorder, stall, and disconnect |
| Reactor harness | [run_scenario](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/mod.rs) | Work queue, byte budget, peer routines, and sequencer |
| Trace-derived checks | [invariants.rs](../../crates/zakura-network/src/zakura/testkit/blocksync_fuzz/invariants.rs) | Request, byte, and progress bounds |
| Monotonic test time | [TestClock](../../crates/zakura-network/src/zakura/testkit/clock.rs) | Existing clock-dependent checks |

Reuse workspace Proptest for legal values and short sequences. These helpers do not establish real
QUIC backpressure or control every runtime interleaving. Use real transport for flow-control claims.

## Frame and decode

Check the 9-byte GetBlocks payload cap, frame/payload discriminator agreement, exact consumption,
count of 1 through 128, and checked end-height arithmetic. The decoded request has no variable-length
collection. Peer-controlled values must not cause decode allocation.

Generate legal start/count values and check production encode/decode round trips. Include explicit
minimum/maximum counts, maximum legal end height, overflow, zero count, truncation, trailing bytes,
and mismatched discriminators. Generated cases supplement deterministic boundaries.

Check response count and body-byte limits independently. A storage result or decoded representation
may consume more memory than its encoded bodies.

## Reservations and terminals

Create authorization before sending GetBlocks. Exercise:

1. A legal range with ascending blocks and matching BlocksDone.
2. Partial completion whose returned count equals consumed blocks, followed by requeueing missing heights.
3. RangeUnavailable with exact start/count before any block.
4. Unsolicited blocks, wrong hashes, duplicates, wrong terminal counts, and duplicate terminals.
5. Overlapping live ranges on one connection.
6. Local reassignment, finality change, or a competing response before the original response arrives.
7. Connection closure with unconsumed authorization.

Local scheduling must not remove authorization. Terminals consume a range once. Known headers do
not validate arbitrary bodies; retain body-commitment and downstream consensus checks.

Use production transitions. A small expected-state model is optional when it clarifies a race.
Do not build a generic model merely to mirror every handler.

## Capacity and ownership

Configure more legal inflight commitments than workers. Verify that executing operations stay
within the worker limit and retained commitments remain bounded.

Saturate output and node-wide workers independently. New response work must wait for required
resources. Releasing capacity must resume processing without a serving-rate timer.

Hold a database operation open with a controlled dependency. Disconnect its peer and drop its
waiter. Verify that the operation keeps its permit until completion. Reconnect repeatedly while
the operation remains blocked. Connection churn must not multiply running operations beyond the
node-wide bound.

Exercise ordinary storage and encoding failures. Finished resources release once, retries return
to scheduling, and local failures do not count as peer violations. Universal panic recovery is not
required for these cleanup checks.

Bound retained results before encoding and queueing. Cover large and small blocks. A bounded
sequential producer may perform several reads; assert memory and execution bounds rather than one
database call per range.

## Bidirectional backpressure

Run endpoints that request and serve concurrently. Saturate output and admission on both sides.
Demonstrate progress without a timeout breaking a cycle between request intake and response receipt.

A paused reader must not trap a response or terminal needed to finish active work. Check another
peer and required service/control streams on the connection. State finite bounds and scheduling
assumptions.

Repeat the relevant case through real QUIC. Stop application request reads and verify bounded
receive buffering, exhaustion of existing stream credit, and required connection-credit headroom.
Resume after capacity returns. In-memory queues alone cannot establish these properties.

## Load and reporting

Run continuous useful requests, repeated unavailable ranges, maximum ranges, tiny responses,
non-reading requesters, and connection churn. Empty responses can cause lookup work without filling
output buffers. Assert bounded execution and opportunities for other runnable peers to progress.

Report peer limits, worker limits, memory/output bounds, and the exercised transport arrangement.
Assert aggregate bounds. Keep diagnostics bounded and save minimal regressions in existing tests
or small fixtures.

Run focused regressions on pull requests and broader generated/load cases on a schedule.
Do not claim exhaustive coverage. A future explorer must state model bounds, production
correspondence, and completion status.

## Activation measurements

On October 4, 2026, the debug build on an ARM64 Mac retained about 44.1 MB for
64,000 waiting GetBlocks commitments in one session after response channels became lazy.
The maximum-session probe retained 22,567,691,400 allocator-requested bytes for 512 sessions
with 64,000 commitments each. Admission took 314.8 seconds. Both probes hold requests before
workers start. They include request bookkeeping, not storage results, QUIC buffers, or process RSS.
The per-request regression ceiling is 1,024 retained bytes, not a memory target.

The full probe is ignored and additionally requires `ZAKURA_LARGE_LOAD_TEST=1`, because CI also
runs ignored tests and this probe needs tens of GB. Run the
`maximum_admitted_getblocks_sessions_have_a_measured_memory_ceiling` test with that variable,
the `zakura-testkit` feature, and `--ignored --nocapture` on a dedicated large host.

After the serving-task supervision fixes, the smaller candidate probe measured waiting
commitments at twice each advertised limit on the same host and debug build:

| Advertised requests per peer | Waiting commitments | Retained bookkeeping bytes |
| --- | --- | --- |
| 32 | 64 | 52,668 |
| 128 | 256 | 184,924 |
| 512 | 1,024 | 714,172 |
| 1,500 | 3,000 | 2,074,140 |
| 32,000 | 64,000 | 44,079,772 |

The incremental allocation probe isolates the shared job queue and its completion tracking.
With 64,000 queued requests, the shared queue retained 1,599,200 bytes without completion tracking
and 23,615,200 bytes with it. The latter adds exactly 344 bytes per request for the watch channel.
The complete GetBlocks session retained 44,079,772 bytes, adding about 320 bytes per request for
range ownership and completion futures, including small fixed session costs. The standalone
completion-receiver vector is allocated before measurement. Response channels remain lazy.
These are allocator-requested bytes, excluding allocator overhead and allocations on other threads.

On October 5, the `compact_waiting_containers` probe retained 4,219,592 bytes for 64,000
requests using the real untracked serving queue plus a range map with integer generations.
That is about 66 bytes per entry and a 2.16 GB extrapolation across 512 sessions. This is a
container measurement, not an implemented completion tracker or a process-memory bound.
It excludes the synchronization and ownership needed to remove completed ranges safely across
out-of-order completion, cancellation, and replacement. Those contracts need implementation and
regression coverage before using this figure to select defaults. No capacity default changed.

The advertisement covers requests outstanding until their endings arrive at the requester.
The server releases a commitment when it queues the ending, while output grants remain charged
through the transport write. Long round trips can therefore require a large advertisement even
with few active storage jobs. Size it from measured storage latency, RTT, response bytes, and
transport buffering, then check adversarial waiting entries at twice that advertisement against
the node budget. Execution concurrency alone does not determine this limit.

Requester reservations have a separate cost. The probe includes the authorization maps, writer
fences, and their retained copies of expected hashes, but excludes scheduler and decoded-block
state. Its input hash vector is allocated before measurement.

| Reservation entries | Expected hashes per entry | Retained bookkeeping bytes |
| --- | --- | --- |
| 16,384 | 1 | 6,632,988 |
| 16,384 | 128 | 73,217,564 |
| 32,768 | 1 | 13,268,828 |
| 32,768 | 128 | 146,437,980 |

Run the ignored `serving_capacity_candidates`, `waiting_commitment_allocation_breakdown`,
`requester_reservation_allocations`, and `compact_waiting_containers` tests with
`ZAKURA_CAPACITY_PROBE=1` and the `zakura-testkit` feature to reproduce this evidence. The candidate runs completed 24,128–36,903 unavailable
responses per second, 29,353–36,029 single-small-block responses per second, and 435–441 configured
128-block responses per second. The latter workload explicitly raises the default one-block
response limit. The probe checks the number of bodies served. These fixtures use an in-process
queue and synthetic storage, without QUIC backpressure or an injected network RTT.

Adding a simulated 500 ms storage delay to each single-block response reduced every candidate
to 36.2–36.3 responses per second, about 0.48 Mbps for these small blocks. The current per-peer
execution limit is 19, derived using the largest response rather than the actual response size.
Nineteen concurrent reads at that latency permit at most 38 responses per second. Increasing the
advertisement from 32 to 32,000 therefore did not improve this workload. This is a controlled
latency experiment, not a measurement of production storage latency.

These measurements do not validate the 32,000 advertised default. A single small-block response
in this fixture averages about 1,644 wire bytes. At that size, the design's twice-bandwidth-delay
allowance at 10 Gbps and 500 ms needs about 760,341 commitments, exceeding the protocol's 32,768
ceiling. The existing 32 MiB QUIC send window also limits transport to about 537 Mbps at that RTT,
before overhead. A lower request count alone cannot establish the target.

Keep activation in draft until representative production-storage and QUIC measurements justify
the capacity default and reconcile the throughput target with response batching and the transport
window. Evaluate compact completion tracking, range batching above the download floor, and
budgeted per-session advertisements together. Any dynamic advertisement must retain capacity for
its highest outstanding promise. Lowering a Status value does not release old commitments.
Measure fixed costs and allocator variation before selecting a memory target, and include the
requester pool separately. Do not silently change those policies or size request counts and
execution slots using the maximum response alone when the default serves one block.

The stream-6 conformance adapter exercises the production layout over QUIC, including simultaneous
serving and exhausted credit. It uses the shared serving harness, so the production GetBlocks
peer-routine QUIC test remains a separate check. Existing synthetic tests without a range source
still exercise the old serving path and do not establish regulated-path coverage.

Version 3 selects the entire regulated contract through its own capability. A real-QUIC test uses
five old peers and one upgraded peer, downloads from an old peer without endings, forces an
overlapping timeout retry, serves actual bodies to every old peer, completes a v3 serving
exchange before the old supplier finishes, and checks prompt Status corrections and replacement. Separate tests preserve v2
replacement admission at its direction cap and its one-second range-correction allowance.
Unexpected header hashes retire locally and retain authorization until cleanup. A short supplier
cooldown survives reconnect without replacing the sequencer's retry episode. Buffered endings
survive local read pauses, while actively withheld endings still retire the connection.
The decoder and shared serving prerequisites do not activate these policies.
