//! Event-bridge core: worker `SubscribeKvEvents` streams -> hash-only
//! index Updates -> one Publish stream to the index. The binary in
//! `bin/bridge.rs` is a thin flag-parsing shell over these; tests drive
//! them in-process.
//!
//! Reconnect semantics mirror the gateway monitor's: resume from the
//! last applied seq; a gap or a backend loss signal (DataLoss /
//! OutOfRange) bumps the holder's EPOCH and restarts from zero — the
//! epoch bump is what makes the restart safe to relay.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::StreamExt;
use smg_grpc_client::{common_proto, tokenspeed_scheduler::TokenSpeedSchedulerClient};
use tokio::sync::mpsc;

use crate::proto::{self, radix_index_client::RadixIndexClient};

/// Cross-task view of each holder's highest epoch the INDEX has acked.
/// `run_publisher` writes it from acks; worker loops consult it, so a
/// restarted bridge (local epoch back at 1) adopts PAST a surviving
/// index's higher epoch instead of having every update silently
/// deduped until the next worker-side cursor loss happens to bump it
/// (liveness review's latent-bug finding — `PublishAck.epoch` exists
/// on the wire precisely for this and was ignored).
#[derive(Clone, Default)]
pub struct EpochLedger(Arc<Mutex<HashMap<String, u64>>>);

impl EpochLedger {
    pub fn observe(&self, holder: &str, epoch: u64) {
        let mut map = self.0.lock().expect("epoch ledger lock");
        let entry = map.entry(holder.to_string()).or_insert(0);
        *entry = (*entry).max(epoch);
    }

    pub fn known(&self, holder: &str) -> u64 {
        self.0
            .lock()
            .expect("epoch ledger lock")
            .get(holder)
            .copied()
            .unwrap_or(0)
    }
}

/// Publisher-side digest state: chains this client has ESTABLISHED
/// with the index (sent full, not since missed), so a re-publish can
/// send a `{tip, len}` digest instead of the full block chain. The
/// stored full `Update` is the replay source: a `digest_miss_tip` ack
/// or a reconnect resends it in full — so a digest is NEVER a silent
/// under-match. Bounded; eviction just forces a future full re-send.
#[derive(Clone, Default)]
pub struct DigestCache(Arc<Mutex<HashMap<u64, proto::Update>>>);

/// Cap on established chains retained per client process. Past it,
/// eviction forces full re-sends — correctness holds, cost rises.
const DIGEST_CACHE_CAP: usize = 131_072;

impl DigestCache {
    /// Decide how to publish `full` (whose chain tip is `tip`): return
    /// `Some(digest)` if the chain is already established (send that
    /// instead), or `None` to send `full` as-is (and record it).
    pub fn plan(&self, tip: u64, len: u32, full: &proto::Update) -> Option<proto::Update> {
        let mut map = self.0.lock().expect("digest cache lock");
        if map.contains_key(&tip) {
            return Some(proto::Update {
                keyspace: full.keyspace.clone(),
                holder: full.holder.clone(),
                epoch: full.epoch,
                seq: 0,
                events: vec![proto::Event {
                    kind: Some(proto::event::Kind::StoredDigest(proto::StoredDigest {
                        parent_seq_hash: None,
                        tip_seq_hash: tip,
                        len,
                    })),
                }],
                added: None,
                dropped: false,
            });
        }
        if map.len() >= DIGEST_CACHE_CAP {
            if let Some(&victim) = map.keys().next() {
                map.remove(&victim);
            }
        }
        map.insert(tip, full.clone());
        None
    }

    /// The full chain to resend for a missed digest tip, if retained.
    pub fn resend(&self, tip: u64) -> Option<proto::Update> {
        self.0.lock().expect("digest cache lock").get(&tip).cloned()
    }

    /// Forget everything: after a reconnect the peer may be a different
    /// replica (or a restarted one) that does not hold these chains, so
    /// the next publishes must re-establish with full sends.
    pub fn reset(&self) {
        self.0.lock().expect("digest cache lock").clear();
    }
}

pub fn keyspace(model: &str, block_size: u32) -> proto::Keyspace {
    keyspace_with_kind(model, block_size, proto::SymbolKind::Tokens)
}

/// Keyspace for an explicit symbol kind. `Tokens` is the token-tree
/// keyspace every token-native path uses; `Bytes` is the separate,
/// server-isolated keyspace for string-mode (raw-byte) placements.
pub fn keyspace_with_kind(
    model: &str,
    block_size: u32,
    symbol_kind: proto::SymbolKind,
) -> proto::Keyspace {
    proto::Keyspace {
        model: model.to_string(),
        symbol_kind: symbol_kind as i32,
        block_size,
        hash_scheme: crate::wire_hash::HASH_SCHEME_V1,
    }
}

pub fn convert_batch(
    batch: &common_proto::KvEventBatch,
    model: &str,
    block_size: u32,
    holder: &str,
    epoch: u64,
) -> proto::Update {
    let events = batch
        .events
        .iter()
        .filter_map(|event| event.data.as_ref())
        .map(|data| {
            let kind = match data {
                common_proto::kv_cache_event::Data::Stored(stored) => {
                    proto::event::Kind::Stored(proto::Stored {
                        parent_seq_hash: stored.parent_block_hash.map(|p| p as u64),
                        blocks: stored
                            .blocks
                            .iter()
                            .map(|b| proto::Block {
                                seq_hash: b.block_hash as u64,
                                content_hash: crate::wire_hash::content_hash(&b.token_ids).0,
                            })
                            .collect(),
                    })
                }
                common_proto::kv_cache_event::Data::Removed(removed) => {
                    proto::event::Kind::Removed(proto::Removed {
                        seq_hashes: removed.block_hashes.iter().map(|&h| h as u64).collect(),
                    })
                }
                common_proto::kv_cache_event::Data::Cleared(_) => proto::event::Kind::Cleared(true),
            };
            proto::Event { kind: Some(kind) }
        })
        .collect();
    proto::Update {
        keyspace: Some(keyspace(model, block_size)),
        holder: holder.to_string(),
        epoch,
        seq: batch.sequence_number,
        events,
        added: None,
        dropped: false,
    }
}

/// One worker's subscription loop: resume on plain failures, epoch-bump
/// on loss signals or sequence gaps. Runs until the publish channel
/// closes or the worker reports Unimplemented.
pub async fn worker_loop(
    worker: String,
    model: String,
    block_size: u32,
    out: mpsc::Sender<proto::Update>,
    ledger: EpochLedger,
) {
    let mut epoch: u64 = 1;
    let mut last_seq: u64 = 0;
    loop {
        // Adopt past whatever epoch the index has acked for this
        // holder: a lower local epoch means every update we send is
        // dead on arrival. Adoption is a new generation, so replay
        // from zero (the resubscribe below starts at `last_seq`).
        let known = ledger.known(&worker);
        if known >= epoch {
            epoch = known + 1;
            last_seq = 0;
        }
        let Ok(client) = TokenSpeedSchedulerClient::connect(&worker).await else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let mut stream = match client.subscribe_kv_events(last_seq).await {
            Ok(stream) => stream,
            Err(status) => {
                match status.code() {
                    // Terminal per the monitor contract.
                    tonic::Code::Unimplemented => {
                        tracing::warn!(%worker, "KV events unimplemented; bridge exits for this worker");
                        return;
                    }
                    // Cursor lost: new generation, replay from zero.
                    tonic::Code::OutOfRange | tonic::Code::DataLoss => {
                        epoch += 1;
                        last_seq = 0;
                    }
                    _ => {}
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        while let Some(batch) = stream.next().await {
            let Ok(batch) = batch else { break };
            if last_seq > 0 && batch.sequence_number <= last_seq {
                continue; // duplicate replay
            }
            if last_seq > 0 && batch.sequence_number > last_seq + 1 {
                // Gap: the ring may have wrapped; new generation.
                epoch += 1;
                last_seq = 0;
                break;
            }
            last_seq = batch.sequence_number;
            let update = convert_batch(&batch, &model, block_size, &worker, epoch);
            if out.send(update).await.is_err() {
                return; // publisher gone; process exiting
            }
            // Mid-stream adoption: acks arrive async, and a healthy
            // stream never reconnects on its own — without this check
            // a stale-epoch bridge would keep feeding deduped updates
            // forever.
            if ledger.known(&worker) >= epoch {
                break; // outer loop adopts and resubscribes from zero
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The publish pump: drain `rx` into one (re)connected Publish stream to
/// `index`. The receiver persists across reconnects, so no update is
/// lost inside the bridge. Returns when all worker loops have ended.
pub async fn run_publisher(rx: mpsc::Receiver<proto::Update>, index: String, ledger: EpochLedger) {
    run_publisher_with_digest(rx, index, ledger, None).await
}

/// As `run_publisher`, plus optional publisher-side digest support:
/// on reconnect the cache is reset (the peer may not hold prior
/// chains), and a `digest_miss_tip` ack resends that chain in full.
pub async fn run_publisher_with_digest(
    mut rx: mpsc::Receiver<proto::Update>,
    index: String,
    ledger: EpochLedger,
    digest: Option<DigestCache>,
) {
    loop {
        let Ok(client) = RadixIndexClient::connect(index.clone()).await else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        };
        let mut client = client
            .max_decoding_message_size(64 * 1024 * 1024)
            .max_encoding_message_size(64 * 1024 * 1024);
        let (fwd_tx, fwd_rx) = mpsc::channel::<proto::Update>(1024);
        let outbound = tokio_stream::wrappers::ReceiverStream::new(fwd_rx);
        let mut acks = match client.publish(tonic::Request::new(outbound)).await {
            Ok(response) => response.into_inner(),
            Err(error) => {
                tracing::warn!(%error, "publish stream failed; retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        // New connection: the peer may be a different or restarted
        // replica, so no chain can be assumed established.
        if let Some(cache) = &digest {
            cache.reset();
        }
        loop {
            tokio::select! {
                item = rx.recv() => match item {
                    Some(update) => {
                        if fwd_tx.send(update).await.is_err() {
                            break;
                        }
                    }
                    // All worker loops ended (fleet torn down).
                    None => return,
                },
                ack = acks.next() => match ack {
                    Some(Ok(ack)) => {
                        ledger.observe(&ack.holder, ack.epoch);
                        // A digest the index could not confirm: resend
                        // the chain in full — never a silent under-match.
                        if let (Some(cache), Some(tip)) = (&digest, ack.digest_miss_tip) {
                            if let Some(full) = cache.resend(tip) {
                                if fwd_tx.send(full).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                    Some(Err(_)) | None => break,
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
