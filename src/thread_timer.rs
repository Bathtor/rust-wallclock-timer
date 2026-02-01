//! Timer implementation that runs on its own thread and uses wall-clock deadlines.
//!
//! This module emits errors via the `log` crate. Provide a logger implementation
//! in your application to see these messages.

use crate::{
    ClosureState,
    timers::{State, WallClockTimer},
};
use crossbeam_channel as channel;
use rustc_hash::FxHashMap;
use snafu::{ResultExt, Snafu};
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, hash_map},
    fmt,
    hash::Hash,
    thread,
    time::{Duration, SystemTime},
};

/// Abstraction over time for testing.
pub trait Clock: Send + 'static {
    /// Return the current wall-clock time.
    fn now(&self) -> SystemTime;
}

/// System clock backed by `SystemTime::now()`.
#[derive(Debug, Clone, Copy)]
pub struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Errors returned by the thread timer APIs.
#[derive(Debug, Snafu)]
pub enum ThreadTimerError {
    /// Failed to spawn the timer thread.
    #[snafu(display("Failed to spawn timer thread: {source}"))]
    SpawnThread { source: std::io::Error },
    /// Failed to send a message to the timer thread.
    #[snafu(display("Failed to send message to timer thread"))]
    SendMessage,
    /// Timer thread panicked while running.
    #[snafu(display("Timer thread panicked while waiting to join"))]
    JoinThread,
}

impl PartialEq for ThreadTimerError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ThreadTimerError::SpawnThread { .. }, _)
            | (_, ThreadTimerError::SpawnThread { .. }) => false,
            (ThreadTimerError::SendMessage, ThreadTimerError::SendMessage) => true,
            (ThreadTimerError::JoinThread, ThreadTimerError::JoinThread) => true,
            _ => false,
        }
    }
}

#[derive(Debug)]
enum TimerMsg<I, O>
where
    I: Hash + Clone + Eq + Ord,
    O: State<Id = I>,
{
    Schedule(TimerEntry<I, O>),
    Cancel(I),
    Stop,
}

/// A shorthand for a reference to a [[ThreadTimer]] with closure actions.
pub type ClosureTimerRef<I> = TimerRef<I, ClosureState<I>>;

/// A reference to a thread timer.
pub struct TimerRef<I, O>
where
    I: Hash + Clone + Eq + Ord,
    O: State<Id = I>,
{
    work_queue: channel::Sender<TimerMsg<I, O>>,
}

impl<I, O> WallClockTimer for TimerRef<I, O>
where
    I: Hash + Clone + Eq + Ord,
    O: State<Id = I>,
{
    type Id = I;
    type State = O;
    type Error = ThreadTimerError;

    fn schedule_at(
        &mut self,
        deadline: SystemTime,
        state: Self::State,
    ) -> Result<(), ThreadTimerError> {
        let entry = TimerEntry { deadline, state };
        self.work_queue
            .send(TimerMsg::Schedule(entry))
            .map_err(|err| {
                log::error!("Failed to send schedule message: {}", err);
                ThreadTimerError::SendMessage
            })
    }

    fn cancel(&mut self, id: Self::Id) -> Result<(), ThreadTimerError> {
        self.work_queue.send(TimerMsg::Cancel(id)).map_err(|err| {
            log::error!("Failed to send cancel message: {}", err);
            ThreadTimerError::SendMessage
        })
    }
}

// Explicit Clone implementation, because O does not need to be Clone for the [[TimerRef]] to be Clone.
impl<I, O> Clone for TimerRef<I, O>
where
    I: Hash + Clone + Eq + Ord,
    O: State<Id = I>,
{
    fn clone(&self) -> Self {
        Self {
            work_queue: self.work_queue.clone(),
        }
    }
}

/// Default value for [[TimerWithThread::new]] `max_wait_time` argument.
pub const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(5);

/// A timer implementation that uses its own thread.
///
/// This instance is essentially the owning handle.
/// Non-owning references can be created with [[TimeWithThread::timer_ref()]] and are always cloneable.
pub struct TimerWithThread<I, O>
where
    I: Hash + Clone + Eq + Ord,
    O: State<Id = I>,
{
    timer_thread: thread::JoinHandle<()>,
    work_queue: channel::Sender<TimerMsg<I, O>>,
}

impl<I, O> TimerWithThread<I, O>
where
    I: Hash + Clone + Eq + Ord + fmt::Debug + Send + 'static,
    O: State<Id = I> + fmt::Debug + Send + 'static,
{
    /// Create a new timer with its own thread.
    ///
    /// `max_wait_time` is the maximum time we wait until we check the clock again,
    /// in case it jumped (e.g. after sleep or due to a timezone change).
    pub fn new(max_wait_time: Duration) -> Result<TimerWithThread<I, O>, ThreadTimerError> {
        Self::new_with_clock(RealClock, max_wait_time)
    }

    /// Create a new timer with its own thread using a custom clock.
    ///
    /// This is mostly meant for testing, but can also be used to supply other clock sources
    /// than [[SystemTime]].
    ///
    /// `max_wait_time` is the maximum time we wait until we check the clock again,
    /// in case it jumped (e.g. after sleep or due to a timezone change).
    pub fn new_with_clock<C>(
        clock: C,
        max_wait_time: Duration,
    ) -> Result<TimerWithThread<I, O>, ThreadTimerError>
    where
        C: Clock,
    {
        let (s, r) = channel::unbounded();
        let handle = thread::Builder::new()
            .name("wallclock-timer-thread".to_string())
            .spawn(move || {
                let timer = TimerThread::new(r, clock, max_wait_time);
                timer.run();
            })
            .context(SpawnThreadSnafu)?;
        Ok(TimerWithThread {
            timer_thread: handle,
            work_queue: s,
        })
    }

    /// Returns a shareable reference to this timer.
    pub fn timer_ref(&self) -> TimerRef<I, O> {
        TimerRef {
            work_queue: self.work_queue.clone(),
        }
    }

    /// Shut this timer down and wait for the thread to join.
    pub fn shutdown(self) -> Result<(), ThreadTimerError> {
        if let Err(send_err) = self.work_queue.send(TimerMsg::Stop) {
            log::error!("Failed to send stop message: {}", send_err);
            // We can't be sure the time_thread will ever finish.
            if self.timer_thread.is_finished() {
                // But if it did, we can print the error message.
                if self.timer_thread.join().is_err() {
                    log::error!("The timer thread panicked. See stderr for more information.");
                }
            } // Otherwise we'll just leak it, rather than risking blocking this thread as well.
            SendMessageSnafu.fail()
        } else {
            self.timer_thread.join().map_err(|_| {
                log::error!("The timer thread panicked. See stderr for more information.");
                JoinThreadSnafu.build()
            })
        }
    }

    /// Same as `shutdown`, but doesn't wait for the thread to join.
    pub fn shutdown_async(&self) -> Result<(), ThreadTimerError> {
        self.work_queue.send(TimerMsg::Stop).map_err(|err| {
            log::error!("Failed to send stop message: {}", err);
            SendMessageSnafu.build()
        })
    }
}

impl<I, O> fmt::Debug for TimerWithThread<I, O>
where
    I: Hash + Clone + Eq + Ord,
    O: State<Id = I>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<TimerWithThread>")
    }
}

impl<I, O> Default for TimerWithThread<I, O>
where
    I: Hash + Clone + Eq + Ord + fmt::Debug + Send + 'static,
    O: State<Id = I> + fmt::Debug + Send + 'static,
{
    fn default() -> Self {
        Self::new(DEFAULT_MAX_WAIT).expect("Failed to create default timer")
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HeapEntry<I> {
    deadline: SystemTime,
    id: I,
}

impl<I> PartialOrd for HeapEntry<I>
where
    I: Eq + Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> Ord for HeapEntry<I>
where
    I: Eq + Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        // match other.deadline.cmp(&self.deadline) {
        //     Ordering::Equal => other.sid.cmp(&self.id),
        //     ord => ord,
        // }
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.id.cmp(&self.id))
    }
}

/// A concrete entry for an outstanding timeout using a wall-clock deadline.
#[derive(Debug)]
struct TimerEntry<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    /// The wall clock deadline at which this should trigger.
    pub deadline: SystemTime,
    /// The information to store along with the timer.
    pub state: O,
}

impl<I, O> TimerEntry<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    /// A reference to the id associated with this entry.
    pub fn id(&self) -> &I {
        self.state.id()
    }
}

struct TimerThread<I, O, C>
where
    I: Hash + Clone + Eq + Ord + fmt::Debug,
    O: State<Id = I> + fmt::Debug,
    C: Clock + Send + 'static,
{
    entry_queue: BinaryHeap<HeapEntry<I>>,
    entries: FxHashMap<I, TimerEntry<I, O>>,
    work_queue: channel::Receiver<TimerMsg<I, O>>,
    running: bool,
    clock: C,
    max_wait_time: Duration,
}

impl<I, O, C> TimerThread<I, O, C>
where
    I: Hash + Clone + Eq + Ord + fmt::Debug,
    O: State<Id = I> + fmt::Debug,
    C: Clock + Send + 'static,
{
    fn new(
        work_queue: channel::Receiver<TimerMsg<I, O>>,
        clock: C,
        max_wait_time: Duration,
    ) -> Self {
        TimerThread {
            entry_queue: BinaryHeap::new(),
            entries: FxHashMap::default(),
            work_queue,
            running: true,
            clock,
            max_wait_time,
        }
    }

    fn run(mut self) {
        'run_loop: while self.running {
            let now = self.clock.now();
            self.process_due(now);
            if !self.running {
                break 'run_loop;
            }

            match self.next_deadline() {
                None => match self.work_queue.recv() {
                    Ok(msg) => self.handle_msg(msg),
                    Err(channel::RecvError) => {
                        log::error!("Channel died, stopping timer thread...");
                        break 'run_loop;
                    }
                },
                Some(deadline) => {
                    if deadline <= now {
                        continue 'run_loop;
                    }
                    // Take a new reading of the clock, since some time could have passed processing the due entries.
                    let wait = deadline
                        .duration_since(self.clock.now())
                        .unwrap_or(Duration::ZERO)
                        .min(self.max_wait_time);
                    match self.work_queue.recv_timeout(wait) {
                        Ok(msg) => self.handle_msg(msg),
                        Err(channel::RecvTimeoutError::Timeout) => {
                            continue 'run_loop;
                        }
                        Err(channel::RecvTimeoutError::Disconnected) => {
                            log::error!("Channel died, stopping timer thread...");
                            break 'run_loop;
                        }
                    }
                }
            }
        }
    }

    fn handle_msg(&mut self, msg: TimerMsg<I, O>) {
        match msg {
            TimerMsg::Stop => {
                log::info!("Timer thread received stop signal. Shutting down...");
                self.running = false
            }
            TimerMsg::Schedule(entry) => self.schedule_entry(entry),
            TimerMsg::Cancel(id) => match self.entries.remove(&id) {
                Some(e) => {
                    log::info!("Cancelled timer entry {e:?}");
                }
                None => {
                    log::warn!(
                        "Could not find timer entry with {id:?} to cancel. It might have expired already?"
                    );
                }
            },
        }
    }

    fn schedule_entry(&mut self, entry: TimerEntry<I, O>) {
        let now = self.clock.now();
        if entry.deadline <= now {
            log::debug!(
                "Triggering entry with id {:?} instead of scheduling, since it's already expired.",
                entry.id()
            );
            entry.state.trigger();
            return;
        }
        let id = entry.id().clone();
        self.insert_entry(id, entry);
    }

    fn insert_entry(&mut self, id: I, entry: TimerEntry<I, O>) {
        match self.entries.entry(id) {
            hash_map::Entry::Occupied(e) => {
                log::error!(
                    "Attempted to re-insert a timer entry with an already existing id. Scheduled timer ids must be unique! Existing entry: {:?}, new entry: {:?}",
                    e,
                    entry
                );
            }
            hash_map::Entry::Vacant(e) => {
                let id = entry.id().clone();
                let deadline = entry.deadline;
                e.insert(entry);
                self.entry_queue.push(HeapEntry { deadline, id });
            }
        }
    }

    fn process_due(&mut self, now: SystemTime) {
        while let Some(scheduled) = self.pop_next_due(now) {
            scheduled.state.trigger();
        }
    }

    #[inline(always)]
    fn next_deadline(&mut self) -> Option<SystemTime> {
        self.entry_queue.peek().map(|entry| entry.deadline)
    }

    fn pop_next_due(&mut self, now: SystemTime) -> Option<TimerEntry<I, O>> {
        if let Some(top) = self.entry_queue.peek() {
            if top.deadline > now {
                return None;
            }
            let entry = self.entry_queue.pop().expect("peeked entry");
            let scheduled = self.entries.remove(&entry.id).expect("entry should exist");
            Some(scheduled)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timers::ClosureTimer;
    use std::sync::{
        Arc,
        Mutex,
        Once,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };

    fn init_logger() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = simple_logger::SimpleLogger::new().init();
        });
    }

    #[derive(Clone)]
    struct MockClock {
        now: Arc<Mutex<SystemTime>>,
    }

    impl MockClock {
        fn new(start: SystemTime) -> Self {
            Self {
                now: Arc::new(Mutex::new(start)),
            }
        }

        fn advance(&self, delta: Duration) {
            let mut guard = self.now.lock().expect("clock lock");
            *guard = guard.checked_add(delta).expect("advance");
        }

        fn set(&self, time: SystemTime) {
            let mut guard = self.now.lock().expect("clock lock");
            *guard = time;
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> SystemTime {
            *self.now.lock().expect("clock lock")
        }
    }

    #[derive(Debug)]
    struct TestState {
        id: u64,
        hits: Arc<AtomicUsize>,
    }

    impl State for TestState {
        type Id = u64;

        fn id(&self) -> &Self::Id {
            &self.id
        }

        fn trigger(self) {
            self.hits.fetch_add(1, AtomicOrdering::SeqCst);
        }
    }

    #[test]
    fn mock_clock_triggers_on_deadline() {
        init_logger();
        let (s, r) = channel::unbounded();
        let mut timer = ThreadTimer::<u64, TestState>::new(r);
        let clock = MockClock::new(SystemTime::UNIX_EPOCH);
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = Arc::new(AtomicUsize::new(0));

        let entry = TimerEntry {
            deadline: SystemTime::UNIX_EPOCH + Duration::from_secs(5),
            state: TestState {
                id: 1,
                hits: hits.clone(),
            },
        };
        let later_entry = TimerEntry {
            deadline: SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            state: TestState {
                id: 2,
                hits: hits2.clone(),
            },
        };

        s.send(TimerMsg::Schedule(entry)).expect("send");
        s.send(TimerMsg::Schedule(later_entry)).expect("send");
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);

        clock.advance(Duration::from_secs(6));
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn wake_on_message_while_waiting_long_timeout() {
        init_logger();
        let timer = TimerWithThread::<u64, crate::timers::ClosureState<u64>>::new().expect("timer");
        let mut tref = timer.timer_ref();

        let far_deadline = SystemTime::now() + Duration::from_secs(60);
        tref.schedule_action_at(1, far_deadline, |_| {})
            .expect("schedule");

        let hits = Arc::new(AtomicUsize::new(0));
        let hits_clone = hits.clone();
        let near_deadline = SystemTime::now() + Duration::from_millis(50);
        tref.schedule_action_at(2, near_deadline, move |_| {
            hits_clone.fetch_add(1, AtomicOrdering::SeqCst);
        })
        .expect("schedule");

        let start = std::time::Instant::now();
        'wait_loop: while start.elapsed() < Duration::from_secs(2) {
            if hits.load(AtomicOrdering::SeqCst) > 0 {
                break 'wait_loop;
            }
            thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(hits.load(AtomicOrdering::SeqCst), 1);
        timer.shutdown().expect("shutdown");
    }

    #[test]
    fn time_jump_forward_triggers_immediately() {
        init_logger();
        let (s, r) = channel::unbounded();
        let mut timer = TimerThread::<u64, TestState>::new(r);
        let clock = MockClock::new(SystemTime::UNIX_EPOCH);
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = Arc::new(AtomicUsize::new(0));

        let entry = WallClockTimerEntry {
            deadline: SystemTime::UNIX_EPOCH + Duration::from_secs(10),
            state: TestState {
                id: 7,
                hits: hits.clone(),
            },
        };
        let far_entry = WallClockTimerEntry {
            deadline: SystemTime::UNIX_EPOCH + Duration::from_secs(90),
            state: TestState {
                id: 8,
                hits: hits2.clone(),
            },
        };
        s.send(TimerMsg::Schedule(entry)).expect("send");
        s.send(TimerMsg::Schedule(far_entry)).expect("send");
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);

        clock.advance(Duration::from_secs(30));
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn time_jump_backward_does_not_trigger_early() {
        init_logger();
        let (s, r) = channel::unbounded();
        let mut timer = TimerThread::<u64, TestState>::new(r);
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let clock = MockClock::new(start);
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = Arc::new(AtomicUsize::new(0));

        let entry = WallClockTimerEntry {
            deadline: start + Duration::from_secs(10),
            state: TestState {
                id: 9,
                hits: hits.clone(),
            },
        };
        let later_entry = WallClockTimerEntry {
            deadline: start + Duration::from_secs(40),
            state: TestState {
                id: 10,
                hits: hits2.clone(),
            },
        };
        s.send(TimerMsg::Schedule(entry)).expect("send");
        s.send(TimerMsg::Schedule(later_entry)).expect("send");
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);

        clock.set(start - Duration::from_secs(30));
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);

        clock.set(start + Duration::from_secs(11));
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);

        clock.set(start + Duration::from_secs(41));
        timer.step(&clock);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 1);
    }

    #[test]
    fn closure_timer_schedules_actions() {
        init_logger();
        let (s, r) = channel::unbounded();
        let mut timer = TimerThread::<u64, crate::timers::ClosureState<u64>>::new(r);
        let clock = MockClock::new(SystemTime::UNIX_EPOCH);

        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = Arc::new(AtomicUsize::new(0));

        let mut tref = TimerRef { work_queue: s };
        let hits_clone = hits.clone();
        tref.schedule_action_at(
            1,
            SystemTime::UNIX_EPOCH + Duration::from_secs(5),
            move |_| {
                hits_clone.fetch_add(1, AtomicOrdering::SeqCst);
            },
        )
        .expect("schedule");
        let hits2_clone = hits2.clone();
        tref.schedule_action_at(
            2,
            SystemTime::UNIX_EPOCH + Duration::from_secs(50),
            move |_| {
                hits2_clone.fetch_add(1, AtomicOrdering::SeqCst);
            },
        )
        .expect("schedule");

        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);

        clock.advance(Duration::from_secs(10));
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);

        clock.advance(Duration::from_secs(50));
        timer.step(&clock);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 1);
    }

    #[test]
    fn cancel_prevents_overdue_trigger_with_multiple_timers() {
        init_logger();
        let (s, r) = channel::unbounded();
        let mut timer = TimerThread::<u64, TestState>::new(r);
        let clock = MockClock::new(SystemTime::UNIX_EPOCH);
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = Arc::new(AtomicUsize::new(0));

        let entry = WallClockTimerEntry {
            deadline: SystemTime::UNIX_EPOCH + Duration::from_secs(5),
            state: TestState {
                id: 100,
                hits: hits.clone(),
            },
        };
        let entry2 = WallClockTimerEntry {
            deadline: SystemTime::UNIX_EPOCH + Duration::from_secs(5),
            state: TestState {
                id: 101,
                hits: hits2.clone(),
            },
        };

        s.send(TimerMsg::Schedule(entry)).expect("send");
        s.send(TimerMsg::Schedule(entry2)).expect("send");
        s.send(TimerMsg::Cancel(101)).expect("send");
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);

        clock.advance(Duration::from_secs(6));
        timer.step(&clock);
        assert_eq!(hits.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(hits2.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn join_thread_error_from_panicking_handler() {
        init_logger();
        let timer = TimerWithThread::<u64, crate::timers::ClosureState<u64>>::new().expect("timer");
        let mut tref = timer.timer_ref();
        tref.schedule_action_at(1, SystemTime::now(), |_| panic!("boom"))
            .expect("schedule");
        thread::sleep(Duration::from_millis(10));
        let err = timer.shutdown().expect_err("expected join error");
        assert_eq!(err, ThreadTimerError::JoinThread);
    }
}
