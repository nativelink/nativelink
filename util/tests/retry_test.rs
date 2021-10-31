// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.
use std::pin::Pin;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use error::{make_err, Code, Error};

use tokio::time::Duration;

use futures::future::ready;
use futures::stream::repeat_with;

use retry::{Retrier, RetryResult};

struct MockDurationIterator {
    duration: Duration,
}

impl MockDurationIterator {
    pub fn new(duration: Duration) -> Self {
        MockDurationIterator { duration: duration }
    }
}

impl Iterator for MockDurationIterator {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        Some(self.duration)
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use pretty_assertions::assert_eq; // Must be declared in every module.

    #[tokio::test]
    async fn retry_simple_success() -> Result<(), Error> {
        let retrier = Retrier::new(Box::new(|_duration| Box::pin(ready(()))));
        let retry_config = MockDurationIterator::new(Duration::from_millis(1));
        let run_count = Arc::new(AtomicI32::new(0));

        let result = Pin::new(&retrier)
            .retry(
                retry_config,
                repeat_with(|| {
                    run_count.fetch_add(1, Ordering::Relaxed);
                    RetryResult::Ok(true)
                }),
            )
            .await?;
        assert_eq!(
            run_count.load(Ordering::Relaxed),
            1,
            "Expected function to be called once"
        );
        assert_eq!(result, true, "Expected result to succeed");

        Ok(())
    }

    #[tokio::test]
    async fn retry_fails_after_3_runs() -> Result<(), Error> {
        let retrier = Retrier::new(Box::new(|_duration| Box::pin(ready(()))));
        let retry_config = MockDurationIterator::new(Duration::from_millis(1)).take(2); // .take() will run X times + 1.
        let run_count = Arc::new(AtomicI32::new(0));

        let result = Pin::new(&retrier)
            .retry(
                retry_config,
                repeat_with(|| {
                    run_count.fetch_add(1, Ordering::Relaxed);
                    RetryResult::<bool>::Retry(make_err!(Code::Unavailable, "Dummy failure",))
                }),
            )
            .await;
        assert_eq!(run_count.load(Ordering::Relaxed), 3, "Expected function to be called");
        assert_eq!(result.is_err(), true, "Expected result to error");
        assert_eq!(
            result.unwrap_err().to_string(),
            "Error { code: Unavailable, messages: [\"Dummy failure\"] }"
        );

        Ok(())
    }

    #[tokio::test]
    async fn retry_success_after_2_runs() -> Result<(), Error> {
        let retrier = Retrier::new(Box::new(|_duration| Box::pin(ready(()))));
        let retry_config = MockDurationIterator::new(Duration::from_millis(1)).take(5); // .take() will run X times + 1.
        let run_count = Arc::new(AtomicI32::new(0));

        let result = Pin::new(&retrier)
            .retry(
                retry_config,
                repeat_with(|| {
                    run_count.fetch_add(1, Ordering::Relaxed);
                    if run_count.load(Ordering::Relaxed) == 2 {
                        return RetryResult::Ok(true);
                    }
                    RetryResult::<bool>::Retry(make_err!(Code::Unavailable, "Dummy failure",))
                }),
            )
            .await?;
        assert_eq!(run_count.load(Ordering::Relaxed), 2, "Expected function to be called");
        assert_eq!(result, true, "Expected result to succeed");

        Ok(())
    }

    // test-prefix/987cc59b2d364596f94bd82f250d02aebb716c3a100163ca784b54a50e0dfde2-142
    #[tokio::test]
    async fn retry_calls_sleep_fn() -> Result<(), Error> {
        const EXPECTED_MS: u64 = 71;
        let sleep_fn_run_count = Arc::new(AtomicI32::new(0));
        let sleep_fn_run_count_copy = sleep_fn_run_count.clone();
        let retrier = Retrier::new(Box::new(move |duration| {
            // Note: Need to make another copy to make the compiler happy.
            let sleep_fn_run_count_copy = sleep_fn_run_count_copy.clone();
            Box::pin(async move {
                // Remember: This function is called only on retries, not the first run.
                sleep_fn_run_count_copy.fetch_add(1, Ordering::Relaxed);
                assert_eq!(duration, Duration::from_millis(EXPECTED_MS));
                ()
            })
        }));

        // s3://blaisebruer-cas-store/test-prefix/987cc59b2d364596f94bd82f250d02aebb716c3a100163ca784b54a50e0dfde2-142
        {
            // Try with retry limit hit.
            let retry_config = MockDurationIterator::new(Duration::from_millis(EXPECTED_MS)).take(5);
            let result = Pin::new(&retrier)
                .retry(
                    retry_config,
                    repeat_with(|| RetryResult::<bool>::Retry(make_err!(Code::Unavailable, "Dummy failure",))),
                )
                .await;

            assert_eq!(result.is_err(), true, "Expected the retry to fail");
            assert_eq!(
                sleep_fn_run_count.load(Ordering::Relaxed),
                5,
                "Expected the sleep_fn to be called twice"
            );
        }
        sleep_fn_run_count.store(0, Ordering::Relaxed); // Reset our counter.
        {
            // Try with 3 retries.
            let retry_config = MockDurationIterator::new(Duration::from_millis(EXPECTED_MS)).take(5);
            let run_count = Arc::new(AtomicI32::new(0));
            let result = Pin::new(&retrier)
                .retry(
                    retry_config,
                    repeat_with(|| {
                        run_count.fetch_add(1, Ordering::Relaxed);
                        // Remember: This function is only called every time, not just retries.
                        // We run the first time, then retry 2 additional times meaning 3 runs.
                        if run_count.load(Ordering::Relaxed) == 3 {
                            return RetryResult::Ok(true);
                        }
                        RetryResult::<bool>::Retry(make_err!(Code::Unavailable, "Dummy failure",))
                    }),
                )
                .await?;

            assert_eq!(result, true, "Expected results to pass");
            assert_eq!(
                sleep_fn_run_count.load(Ordering::Relaxed),
                2,
                "Expected the sleep_fn to be called twice"
            );
        }

        Ok(())
    }
}
