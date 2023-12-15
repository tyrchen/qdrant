use std::ops::Deref as _;
use std::time::Duration;

use futures::stream::FuturesUnordered;
use futures::{FutureExt as _, StreamExt as _};
use itertools::Itertools as _;

use super::{ReplicaSetState, ReplicaState, ShardReplicaSet};
use crate::operations::point_ops::WriteOrdering;
use crate::operations::types::{CollectionError, CollectionResult, UpdateResult};
use crate::operations::CollectionUpdateOperations;
use crate::shards::shard::PeerId;
use crate::shards::shard_trait::ShardOperation as _;

const DEFAULT_SHARD_DEACTIVATION_TIMEOUT: Duration = Duration::from_secs(30);

impl ShardReplicaSet {
    /// Update local shard if any without forwarding to remote shards
    pub async fn update_local(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<Option<UpdateResult>> {
        if let Some(local_shard) = &*self.local.read().await {
            match self.peer_state(&self.this_peer_id()) {
                Some(ReplicaState::Active | ReplicaState::Partial | ReplicaState::Initializing) => {
                    Ok(Some(local_shard.get().update(operation, wait).await?))
                }
                Some(ReplicaState::Listener) => {
                    Ok(Some(local_shard.get().update(operation, false).await?))
                }
                Some(ReplicaState::PartialSnapshot | ReplicaState::Dead) | None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub async fn update_with_consistency(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        match self.leader_peer_for_update(ordering) {
            None => Err(CollectionError::service_error(format!(
                "Cannot update shard {}:{} with {ordering:?} ordering because no leader could be selected",
                self.collection_id, self.shard_id
            ))),
            Some(leader_peer) => {
                // If we are the leader, run the update from this replica set
                if leader_peer == self.this_peer_id() {
                    // lock updates if ordering is medium or strong
                    let _guard = match ordering {
                        WriteOrdering::Weak => None, // no locking required
                        WriteOrdering::Medium | WriteOrdering::Strong => Some(self.write_ordering_lock.lock().await), // one request at a time
                    };
                    self.update(operation, wait).await
                } else {
                    // forward the update to the designated leader
                    self.forward_update(leader_peer, operation, wait, ordering)
                        .await
                        .map_err(|err| {
                            if err.is_transient() {
                                // Deactivate the peer if forwarding failed with transient error
                                self.add_locally_disabled(leader_peer);

                                // return service error
                                CollectionError::service_error(format!(
                                    "Failed to apply update with {ordering:?} ordering via leader peer {leader_peer}: {err}"
                                ))
                            } else {
                                err
                            }
                        })
                }
            }
        }
    }

    /// Designated a leader replica for the update based on the WriteOrdering
    fn leader_peer_for_update(&self, ordering: WriteOrdering) -> Option<PeerId> {
        match ordering {
            WriteOrdering::Weak => Some(self.this_peer_id()), // no requirement for consistency
            WriteOrdering::Medium => self.highest_alive_replica_peer_id(), // consistency with highest alive replica
            WriteOrdering::Strong => self.highest_replica_peer_id(), // consistency with highest replica
        }
    }

    fn highest_alive_replica_peer_id(&self) -> Option<PeerId> {
        let read_lock = self.replica_state.read();
        let peer_ids = read_lock.peers.keys().cloned().collect::<Vec<_>>();
        drop(read_lock);

        peer_ids
            .into_iter()
            .filter(|peer_id| self.peer_is_active(peer_id)) // re-acquire replica_state read lock
            .max()
    }

    fn highest_replica_peer_id(&self) -> Option<PeerId> {
        self.replica_state.read().peers.keys().max().cloned()
    }

    async fn update(
        &self,
        operation: CollectionUpdateOperations,
        wait: bool,
    ) -> CollectionResult<UpdateResult> {
        let all_res: Vec<Result<_, _>> = {
            let remotes = self.remotes.read().await;
            let local = self.local.read().await;
            let this_peer_id = self.this_peer_id();

            // target all remote peers that can receive updates
            let active_remote_shards: Vec<_> = remotes
                .iter()
                .filter(|rs| self.peer_is_active_or_pending(&rs.peer_id))
                .collect();

            // local is defined AND the peer itself can receive updates
            let local_is_updatable =
                local.is_some() && self.peer_is_active_or_pending(&this_peer_id);

            if active_remote_shards.is_empty() && !local_is_updatable {
                return Err(CollectionError::service_error(format!(
                    "The replica set for shard {} on peer {} has no active replica",
                    self.shard_id, this_peer_id
                )));
            }

            let mut update_futures = Vec::with_capacity(active_remote_shards.len() + 1);

            if let Some(local) = local.deref() {
                if self.peer_is_active_or_pending(&this_peer_id) {
                    let local_wait =
                        if self.peer_state(&this_peer_id) == Some(ReplicaState::Listener) {
                            false
                        } else {
                            wait
                        };

                    let operation = operation.clone();

                    let local_update = async move {
                        local
                            .get()
                            .update(operation, local_wait)
                            .await
                            .map(|ok| (this_peer_id, ok))
                            .map_err(|err| (this_peer_id, err))
                    };

                    update_futures.push(local_update.left_future());
                }
            }

            for remote in active_remote_shards {
                let operation = operation.clone();

                let remote_update = async move {
                    remote
                        .update(operation, wait)
                        .await
                        .map(|ok| (remote.peer_id, ok))
                        .map_err(|err| (remote.peer_id, err))
                };

                update_futures.push(remote_update.right_future());
            }

            match self.shared_storage_config.update_concurrency {
                Some(concurrency) => {
                    futures::stream::iter(update_futures)
                        .buffer_unordered(concurrency.get())
                        .collect()
                        .await
                }

                None => FuturesUnordered::from_iter(update_futures).collect().await,
            }
        };

        let total_results = all_res.len();

        let write_consistency_factor = self
            .collection_config
            .read()
            .await
            .params
            .write_consistency_factor
            .get() as usize;

        let minimal_success_count = write_consistency_factor.min(total_results);

        let (successes, failures): (Vec<_>, Vec<_>) = all_res.into_iter().partition_result();

        // Notify consensus about failures if:
        // 1. There is at least one success, otherwise it might be a problem of sending node
        // 2. ???

        let failure_error = if let Some((peer_id, collection_error)) = failures.first() {
            format!("Failed peer: {}, error: {}", peer_id, collection_error)
        } else {
            "".to_string()
        };

        if successes.len() >= minimal_success_count {
            let wait_for_deactivation =
                self.handle_failed_replicas(&failures, &self.replica_state.read());

            // report all failing peers to consensus
            if wait && wait_for_deactivation && !failures.is_empty() {
                // ToDo: allow timeout configuration in API
                let timeout = DEFAULT_SHARD_DEACTIVATION_TIMEOUT;

                let replica_state = self.replica_state.clone();
                let peer_ids: Vec<_> = failures.iter().map(|(peer_id, _)| *peer_id).collect();

                let shards_disabled = tokio::task::spawn_blocking(move || {
                    replica_state.wait_for(
                        |state| {
                            peer_ids.iter().all(|peer_id| {
                                state
                                    .peers
                                    .get(peer_id)
                                    .map(|state| state != &ReplicaState::Active)
                                    .unwrap_or(true) // not found means that peer is dead
                            })
                        },
                        DEFAULT_SHARD_DEACTIVATION_TIMEOUT,
                    )
                })
                .await?;

                if !shards_disabled {
                    return Err(CollectionError::service_error(format!(
                        "Some replica of shard {} failed to apply operation and deactivation \
                         timed out after {} seconds. Consistency of this update is not guaranteed. Please retry. {failure_error}",
                        self.shard_id, timeout.as_secs()
                    )));
                }
            }
        }

        if !failures.is_empty() && successes.len() < minimal_success_count {
            // completely failed - report error to user
            let (_peer_id, err) = failures.into_iter().next().expect("failures is not empty");
            return Err(err);
        }

        if !successes
            .iter()
            .any(|(peer_id, _)| self.peer_is_active(peer_id))
        {
            return Err(CollectionError::service_error(format!(
                "Failed to apply operation to at least one `Active` replica. \
                 Consistency of this update is not guaranteed. Please retry. {failure_error}"
            )));
        }

        // there are enough successes, return the first one
        let (_, res) = successes
            .into_iter()
            .next()
            .expect("successes is not empty");

        Ok(res)
    }

    fn peer_is_active_or_pending(&self, peer_id: &PeerId) -> bool {
        let res = match self.peer_state(peer_id) {
            Some(ReplicaState::Active) => true,
            Some(ReplicaState::Partial) => true,
            Some(ReplicaState::Initializing) => true,
            Some(ReplicaState::Dead) => false,
            Some(ReplicaState::Listener) => true,
            Some(ReplicaState::PartialSnapshot) => false,
            None => false,
        };
        res && !self.is_locally_disabled(peer_id)
    }

    fn handle_failed_replicas(
        &self,
        failures: &Vec<(PeerId, CollectionError)>,
        state: &ReplicaSetState,
    ) -> bool {
        let mut wait_for_deactivation = false;

        for (peer_id, err) in failures {
            log::warn!(
                "Failed to update shard {}:{} on peer {}, error: {}",
                self.collection_id,
                self.shard_id,
                peer_id,
                err
            );

            let Some(&peer_state) = state.get_peer_state(peer_id) else {
                continue;
            };

            if peer_state != ReplicaState::Active && peer_state != ReplicaState::Initializing {
                continue;
            }

            if err.is_transient() || peer_state == ReplicaState::Initializing {
                // If the error is transient, we should not deactivate the peer
                // before allowing other operations to continue.
                // Otherwise, the failed node can become responsive again, before
                // the other nodes deactivate it, so the storage might be inconsistent.
                wait_for_deactivation = true;
            }

            log::debug!(
                "Deactivating peer {} because of failed update of shard {}:{}",
                peer_id,
                self.collection_id,
                self.shard_id
            );

            self.add_locally_disabled(*peer_id);
        }

        wait_for_deactivation
    }
    /// Forward update to the leader replica
    async fn forward_update(
        &self,
        leader_peer: PeerId,
        operation: CollectionUpdateOperations,
        wait: bool,
        ordering: WriteOrdering,
    ) -> CollectionResult<UpdateResult> {
        let remotes_guard = self.remotes.read().await;
        let remote_leader = remotes_guard.iter().find(|r| r.peer_id == leader_peer);

        match remote_leader {
            Some(remote_leader) => {
                remote_leader
                    .forward_update(operation, wait, ordering)
                    .await
            }
            None => Err(CollectionError::service_error(format!(
                "Cannot forward update to shard {} because was removed from the replica set",
                self.shard_id
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::Arc;

    use common::cpu::CpuBudget;
    use segment::types::Distance;
    use tempfile::{Builder, TempDir};
    use tokio::runtime::Handle;
    use tokio::sync::RwLock;

    use super::*;
    use crate::config::*;
    use crate::operations::types::{VectorParams, VectorsConfig};
    use crate::optimizers_builder::OptimizersConfig;
    use crate::shards::replica_set::{AbortShardTransfer, ChangePeerState};

    #[tokio::test]
    async fn test_highest_replica_peer_id() {
        let collection_dir = Builder::new().prefix("test_collection").tempdir().unwrap();
        let rs = new_shard_replica_set(&collection_dir).await;

        assert_eq!(rs.highest_replica_peer_id(), Some(5));
        // at build time the replicas are all dead, they need to be activated
        assert_eq!(rs.highest_alive_replica_peer_id(), None);

        rs.set_replica_state(&1, ReplicaState::Active).unwrap();
        rs.set_replica_state(&3, ReplicaState::Active).unwrap();
        rs.set_replica_state(&4, ReplicaState::Active).unwrap();
        rs.set_replica_state(&5, ReplicaState::Partial).unwrap();

        assert_eq!(rs.highest_replica_peer_id(), Some(5));
        assert_eq!(rs.highest_alive_replica_peer_id(), Some(4));
    }

    const TEST_OPTIMIZERS_CONFIG: OptimizersConfig = OptimizersConfig {
        deleted_threshold: 0.9,
        vacuum_min_vector_number: 1000,
        default_segment_number: 2,
        max_segment_size: None,
        memmap_threshold: None,
        indexing_threshold: Some(50_000),
        flush_interval_sec: 30,
        max_optimization_threads: Some(2),
    };

    async fn new_shard_replica_set(collection_dir: &TempDir) -> ShardReplicaSet {
        let update_runtime = Handle::current();
        let search_runtime = Handle::current();

        let wal_config = WalConfig {
            wal_capacity_mb: 1,
            wal_segments_ahead: 0,
        };

        let collection_params = CollectionParams {
            vectors: VectorsConfig::Single(VectorParams {
                size: NonZeroU64::new(4).unwrap(),
                distance: Distance::Dot,
                hnsw_config: None,
                quantization_config: None,
                on_disk: None,
            }),
            shard_number: NonZeroU32::new(4).unwrap(),
            replication_factor: NonZeroU32::new(3).unwrap(),
            write_consistency_factor: NonZeroU32::new(2).unwrap(),
            ..CollectionParams::empty()
        };

        let config = CollectionConfig {
            params: collection_params,
            optimizer_config: TEST_OPTIMIZERS_CONFIG.clone(),
            wal_config,
            hnsw_config: Default::default(),
            quantization_config: None,
        };

        let shared_config = Arc::new(RwLock::new(config.clone()));
        let remotes = HashSet::from([2, 3, 4, 5]);
        ShardReplicaSet::build(
            1,
            "test_collection".to_string(),
            1,
            false,
            remotes,
            dummy_on_replica_failure(),
            dummy_abort_shard_transfer(),
            collection_dir.path(),
            shared_config,
            Default::default(),
            Default::default(),
            update_runtime,
            search_runtime,
            CpuBudget::default(),
            None,
        )
        .await
        .unwrap()
    }

    fn dummy_on_replica_failure() -> ChangePeerState {
        Arc::new(move |_peer_id, _shard_id| {})
    }

    fn dummy_abort_shard_transfer() -> AbortShardTransfer {
        Arc::new(|_shard_transfer, _reason| {})
    }
}
