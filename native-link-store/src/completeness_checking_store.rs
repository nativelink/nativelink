// Copyright 2023 The Native Link Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;
use std::sync::Arc;
use std::{iter, mem};

use async_trait::async_trait;
use error::{make_err, Code, Error, ResultExt};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::{select, FutureExt, TryFutureExt};
use native_link_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use native_link_util::common::DigestInfo;
use native_link_util::store_trait::{Store, UploadSizeInfo};
use parking_lot::Mutex;
use proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, OutputDirectory as ProtoOutputDirectory, Tree as ProtoTree,
};
use tokio::sync::Notify;
use tracing::warn;

use crate::ac_utils::{get_and_decode_digest, get_size_and_decode_digest};

pub struct CompletenessCheckingStore {
    cas_store: Arc<dyn Store>,
    ac_store: Arc<dyn Store>,
}

impl CompletenessCheckingStore {
    pub fn new(ac_store: Arc<dyn Store>, cas_store: Arc<dyn Store>) -> Self {
        CompletenessCheckingStore { cas_store, ac_store }
    }
}

/// Given a proto action result, return all relevant digests and
/// output directories that need to be checked.
fn get_digests_and_output_dirs(
    action_result: ProtoActionResult,
) -> Result<(Vec<DigestInfo>, Vec<ProtoOutputDirectory>), Error> {
    // TODO(allada) When `try_collect()` is stable we can use it instead.
    let mut digest_iter = action_result
        .output_files
        .into_iter()
        .filter_map(|file| file.digest.map(DigestInfo::try_from))
        .chain(action_result.stdout_digest.map(DigestInfo::try_from))
        .chain(action_result.stderr_digest.map(DigestInfo::try_from));
    let mut digest_infos = Vec::with_capacity(digest_iter.size_hint().1.unwrap_or(0));
    digest_iter
        .try_for_each(|maybe_digest| {
            digest_infos.push(maybe_digest?);
            Result::<_, Error>::Ok(())
        })
        .err_tip(|| "Some digests could not be converted to DigestInfos")?;
    Ok((digest_infos, action_result.output_directories))
}

/// Given a list of output directories recursively get all digests
/// that need to be checked and pass them into `handle_digest_infos_fn`
/// as they are found.
async fn check_output_directories(
    cas_store: Pin<&dyn Store>,
    output_directories: Vec<ProtoOutputDirectory>,
    handle_digest_infos_fn: &impl Fn(Vec<DigestInfo>),
) -> Result<(), Error> {
    let mut futures = FuturesUnordered::new();

    let tree_digests = output_directories
        .into_iter()
        .filter_map(|output_dir| output_dir.tree_digest.map(DigestInfo::try_from));
    for maybe_tree_digest in tree_digests {
        let tree_digest =
            maybe_tree_digest.err_tip(|| "Could not decode tree digest CompletenessCheckingStore::has")?;
        futures.push(async move {
            let tree = get_and_decode_digest::<ProtoTree>(cas_store, &tree_digest).await?;
            // TODO(allada) When `try_collect()` is stable we can use it instead.
            let mut digest_iter = tree.children.into_iter().chain(tree.root).flat_map(|dir| {
                dir.files
                    .into_iter()
                    .filter_map(|f| f.digest.map(DigestInfo::try_from))
                    .chain(
                        dir.directories
                            .into_iter()
                            .filter_map(|d| d.digest.map(DigestInfo::try_from)),
                    )
            });

            let mut digest_infos = Vec::with_capacity(digest_iter.size_hint().1.unwrap_or(0));
            digest_iter
                .try_for_each(|maybe_digest| {
                    digest_infos.push(maybe_digest?);
                    Result::<_, Error>::Ok(())
                })
                .err_tip(|| "Expected digest to exist and be convertable")?;
            handle_digest_infos_fn(digest_infos);
            Ok(())
        });
    }

    while let Some(result) = futures.next().await {
        match result {
            Ok(()) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

async fn inner_has_with_results(
    ac_store: Pin<&dyn Store>,
    cas_store: Pin<&dyn Store>,
    action_result_digests: &[DigestInfo],
    results: &mut [Option<usize>],
) -> Result<(), Error> {
    // Holds shared state between the different futures.
    // This is how get around lifetime issues.
    struct State<'a> {
        results: &'a mut [Option<usize>],
        digests_to_check: Vec<DigestInfo>,
        digests_to_check_idxs: Vec<usize>,
        notify: Arc<Notify>,
        done: bool,
    }
    // Note: In theory Mutex is not needed, but lifetimes are
    // very tricky to get right here. Since we are using parking_lot
    // and we are guaranteed to never have lock collisions, it should
    // be nearly as fast as a few atomic operations.
    let state_mux = &Mutex::new(State {
        results,
        digests_to_check: Vec::new(),
        digests_to_check_idxs: Vec::new(),
        // Note: Any time `digests_to_check` or `digests_to_check_idxs` is
        // modified we must notify the subscriber here.
        notify: Arc::new(Notify::new()),
        done: false,
    });

    let mut futures = action_result_digests
        .iter()
        .enumerate()
        .map(|(i, digest)| {
            async move {
                // Note: We don't err_tip here because often have NotFound here which is ok.
                let (action_result, size) = get_size_and_decode_digest::<ProtoActionResult>(ac_store, digest).await?;

                let (mut digest_infos, output_directories) = get_digests_and_output_dirs(action_result)?;

                {
                    // We immediately set the size of the digest here. Later we will unset it if
                    // we find that the digest has missing outputs.
                    state_mux.lock().results[i] = Some(size);

                    let mut state = state_mux.lock();
                    let rep_len = digest_infos.len();
                    if state.digests_to_check.is_empty() {
                        // Hot path: Most actions only have files and only one digest
                        // requested to be checked. So we can avoid the heap allocation
                        // by just swapping out our container's stack if our pending_digests
                        // is empty.
                        mem::swap(&mut state.digests_to_check, &mut digest_infos);
                    } else {
                        state.digests_to_check.extend(digest_infos);
                    }
                    state.digests_to_check_idxs.extend(iter::repeat(i).take(rep_len));
                    state.notify.notify_one();
                }

                // Hot path: It is very common for no output directories to be defined.
                // So we can avoid any needless work by early returning.
                if output_directories.is_empty() {
                    return Ok(());
                }

                check_output_directories(cas_store, output_directories, &move |digest_infos| {
                    let mut state = state_mux.lock();
                    let rep_len = digest_infos.len();
                    state.digests_to_check.extend(digest_infos);
                    state.digests_to_check_idxs.extend(iter::repeat(i).take(rep_len));
                    state.notify.notify_one();
                })
                .await?;

                Result::<(), Error>::Ok(())
            }
            // Add a tip to the error to help with debugging and the index of the
            // digest that failed so we know which one to unset.
            .map_err(move |mut e| {
                if e.code != Code::NotFound {
                    e = e.append("Error checking existance of digest in CompletenessCheckingStore::has");
                }
                (e, i)
            })
        })
        .collect::<FuturesUnordered<_>>();

    // This future will wait for the notify to be notified and then
    // check the CAS store for the digest's existance.
    // For optimization reasons we only allow one outstanding call to
    // the underlying `has_with_results()` at a time. This is because
    // we want to give the ability for stores to batch requests together
    // whenever possible.
    // The most common case is only one notify will ever happen.
    let check_existance_fut = async {
        let mut has_results = vec![];
        let notify = state_mux.lock().notify.clone();
        loop {
            notify.notified().await;
            let (digests, indexes) = {
                let mut state = state_mux.lock();
                if state.done {
                    if state.digests_to_check.is_empty() {
                        break;
                    }
                    // Edge case: It is possible for our `digest_to_check` to have had
                    // data added, `notify_one` called, then immediately `done` set to
                    // true. We protect ourselves by checking if we have digests if done
                    // is set, and if we do, let ourselves know to run again, but continue
                    // processing the data.
                    notify.notify_one();
                }
                (
                    mem::take(&mut state.digests_to_check),
                    mem::take(&mut state.digests_to_check_idxs),
                )
            };
            assert!(
                digests.len() == indexes.len(),
                "Expected sizes to match in CompletenessCheckingStore::has"
            );

            // Recycle our results vector to avoid needless allocations.
            has_results.clear();
            has_results.resize(digests.len(), None);
            cas_store
                .has_with_results(&digests, &mut has_results[..])
                .await
                .err_tip(|| "Error calling has_with_results() inside CompletenessCheckingStore::has")?;
            let missed_indexes = has_results
                .iter()
                .zip(indexes)
                .filter_map(|(r, index)| r.map_or_else(|| Some(index), |_| None));
            {
                let mut state = state_mux.lock();
                for index in missed_indexes {
                    state.results[index] = None;
                }
            }
        }
        Result::<(), Error>::Ok(())
    }
    .fuse();
    tokio::pin!(check_existance_fut);

    loop {
        // Poll both futures at the same time.
        select! {
            r = check_existance_fut => {
                return Err(make_err!(
                    Code::Internal,
                    "CompletenessCheckingStore's check_existance_fut ended unexpectedly {r:?}"
                ));
            }
            maybe_result = futures.next() => {
                match maybe_result {
                    Some(Ok(())) => {}
                    Some(Err((e, i))) => {
                        state_mux.lock().results[i] = None;
                        // Note: Don't return the errors. We just flag the result as
                        // missing but show a warning if it's not a NotFound.
                        if e.code != Code::NotFound {
                            warn!("{e:?}");
                        }
                    }
                    None => {
                        // We are done, so flag it done and ensure we notify the
                        // subscriber future.
                        {
                            let mut state = state_mux.lock();
                            state.done = true;
                            state.notify.notify_one();
                        }
                        check_existance_fut
                            .await
                            .err_tip(|| "CompletenessCheckingStore's check_existance_fut ended unexpectedly on last await")?;
                        return Ok(());
                    }
                }
            }
        }
    }
    // Unreachable.
}

#[async_trait]
impl Store for CompletenessCheckingStore {
    async fn has_with_results(
        self: Pin<&Self>,
        action_result_digests: &[DigestInfo],
        results: &mut [Option<usize>],
    ) -> Result<(), Error> {
        inner_has_with_results(
            Pin::new(self.ac_store.as_ref()),
            Pin::new(self.cas_store.as_ref()),
            action_result_digests,
            results,
        )
        .await
    }

    async fn update(
        self: Pin<&Self>,
        digest: DigestInfo,
        reader: DropCloserReadHalf,
        size_info: UploadSizeInfo,
    ) -> Result<(), Error> {
        Pin::new(self.ac_store.as_ref()).update(digest, reader, size_info).await
    }

    async fn get_part_ref(
        self: Pin<&Self>,
        digest: DigestInfo,
        writer: &mut DropCloserWriteHalf,
        offset: usize,
        length: Option<usize>,
    ) -> Result<(), Error> {
        let ac_store = Pin::new(self.ac_store.as_ref());
        let results = &mut [None];
        inner_has_with_results(ac_store, Pin::new(self.cas_store.as_ref()), &[digest], results)
            .await
            .err_tip(|| "when calling CompletenessCheckingStore::get_part_ref")?;
        if results[0].is_none() {
            return Err(make_err!(
                Code::NotFound,
                "Digest found, but not all parts were found in CompletenessCheckingStore::get_part_ref"
            ));
        }
        ac_store.get_part_ref(digest, writer, offset, length).await
    }

    fn inner_store(self: Arc<Self>, _digest: Option<DigestInfo>) -> Arc<dyn Store> {
        self
    }

    fn as_any(self: Arc<Self>) -> Box<dyn std::any::Any + Send> {
        Box::new(self)
    }
}