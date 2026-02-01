use crate::timers::WallClockTimer;
use snafu::prelude::*;
use std::time::Duration;

/// Errors that can occur when scheduling with `chrono::DateTime`.
#[derive(Debug, Snafu)]
pub enum ChronoTimerError<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    /// The `chrono` time could not be converted into a `SystemTime`.
    #[snafu(display("chrono::DateTime is out of range for SystemTime"))]
    ChronoOutOfRange,
    /// The underlying timer implementation returned an error.
    #[snafu(display("timer error while scheduling chrono DateTime: {source}"))]
    ImplementationSpecific { source: E },
}

/// Convenience API for scheduling with `chrono::DateTime`.
pub trait ChronoTimer: WallClockTimer {
    /// Schedule the `state` to be triggered at the given `chrono` deadline.
    ///
    /// Note that these get eagerly converted to system time and are not stable with respect to
    /// timezone changes.
    fn schedule_at_datetime<Tz>(
        &mut self,
        deadline: chrono::DateTime<Tz>,
        state: Self::State,
    ) -> Result<(), ChronoTimerError<Self::Error>>
    where
        Tz: chrono::TimeZone,
        Tz::Offset: Send + Sync;
}

impl<T> ChronoTimer for T
where
    T: WallClockTimer,
{
    fn schedule_at_datetime<Tz>(
        &mut self,
        deadline: chrono::DateTime<Tz>,
        state: Self::State,
    ) -> Result<(), ChronoTimerError<Self::Error>>
    where
        Tz: chrono::TimeZone,
        Tz::Offset: Send + Sync,
    {
        let delta = deadline
            .with_timezone(&chrono::Utc)
            .signed_duration_since(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
        // Drop the chrono error type, it contains no more information anyway.
        let duration = delta.to_std().ok().context(ChronoOutOfRangeSnafu)?;
        let deadline = std::time::UNIX_EPOCH
            .checked_add(Duration::from_secs(duration.as_secs()))
            .and_then(|t| t.checked_add(Duration::from_nanos(duration.subsec_nanos() as u64)))
            .context(ChronoOutOfRangeSnafu)?;
        self.schedule_at(deadline, state)
            .context(ImplementationSpecificSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timers::{State, WallClockTimer};

    #[derive(Debug)]
    struct TestState {
        id: u64,
    }

    impl State for TestState {
        type Id = u64;

        fn id(&self) -> &Self::Id {
            &self.id
        }

        fn trigger(self) {}
    }

    #[derive(Debug)]
    struct StubTimer {
        last_deadline: Option<std::time::SystemTime>,
    }

    #[derive(Debug)]
    struct StubError;

    impl std::fmt::Display for StubError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "stub")
        }
    }

    impl std::error::Error for StubError {}

    impl WallClockTimer for StubTimer {
        type Id = u64;
        type State = TestState;
        type Error = StubError;

        fn schedule_at(
            &mut self,
            deadline: std::time::SystemTime,
            _state: Self::State,
        ) -> Result<(), Self::Error> {
            self.last_deadline = Some(deadline);
            Ok(())
        }

        fn cancel(&mut self, _id: Self::Id) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn chrono_datetime_converts_to_systemtime() {
        let mut timer = StubTimer {
            last_deadline: None,
        };
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(1, 0).expect("timestamp");
        timer
            .schedule_at_datetime(dt, TestState { id: 1 })
            .expect("schedule");
        let expected = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1);
        assert_eq!(timer.last_deadline, Some(expected));
    }
}
