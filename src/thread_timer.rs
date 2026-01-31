//! Timer implementation that runs on its own thread and uses wall-clock deadlines.

use crate::timers::{State, WallClockTimer, WallClockTimerEntry};
use crossbeam_channel as channel;
use rustc_hash::FxHashMap;
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
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

#[derive(Debug)]
enum TimerMsg<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    Schedule(WallClockTimerEntry<I, O>),
    Cancel(I),
    Stop,
}

/// A reference to a thread timer.
pub struct TimerRef<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    work_queue: channel::Sender<TimerMsg<I, O>>,
}

impl<I, O> WallClockTimer for TimerRef<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    type Id = I;
    type State = O;

    fn schedule_at(&mut self, deadline: SystemTime, state: Self::State) {
        let entry = WallClockTimerEntry { deadline, state };
        self.work_queue
            .send(TimerMsg::Schedule(entry))
            .unwrap_or_else(|e| eprintln!("Could not send Schedule msg: {:?}", e));
    }

    fn cancel(&mut self, id: &Self::Id) {
        self.work_queue
            .send(TimerMsg::Cancel(id.clone()))
            .unwrap_or_else(|e| eprintln!("Could not send Cancel msg: {:?}", e));
    }
}

impl<I, O> Clone for TimerRef<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    fn clone(&self) -> Self {
        Self {
            work_queue: self.work_queue.clone(),
        }
    }
}

/// A timer implementation that uses its own thread.
pub struct TimerWithThread<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    timer_thread: thread::JoinHandle<()>,
    work_queue: channel::Sender<TimerMsg<I, O>>,
}

impl<I, O> TimerWithThread<I, O>
where
    I: Hash + Clone + Eq + fmt::Debug + Send + 'static,
    O: State<Id = I> + fmt::Debug + Send + 'static,
{
    /// Create a new timer with its own thread.
    pub fn new() -> std::io::Result<TimerWithThread<I, O>> {
        Self::new_with_clock(RealClock)
    }

    /// Create a new timer with its own thread using a custom clock.
    pub fn new_with_clock<C>(clock: C) -> std::io::Result<TimerWithThread<I, O>>
    where
        C: Clock,
    {
        let (s, r) = channel::unbounded();
        let handle = thread::Builder::new()
            .name("wallclock-timer-thread".to_string())
            .spawn(move || {
                let timer = TimerThread::new(r);
                timer.run_with_clock(clock);
            })?;
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
        self.work_queue
            .send(TimerMsg::Stop)
            .unwrap_or_else(|e| eprintln!("Could not send Stop msg: {:?}", e));
        match self.timer_thread.join() {
            Ok(_) => Ok(()),
            Err(_) => Err(ThreadTimerError::CouldNotJoinThread),
        }
    }

    /// Same as `shutdown`, but doesn't wait for the thread to join.
    pub fn shutdown_async(&self) -> Result<(), ThreadTimerError> {
        self.work_queue
            .send(TimerMsg::Stop)
            .unwrap_or_else(|e| eprintln!("Could not send Stop msg: {:?}", e));
        Ok(())
    }
}

impl<I, O> fmt::Debug for TimerWithThread<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<TimerWithThread>")
    }
}

/// Errors that can occur when stopping the timer thread.
#[derive(Debug)]
pub enum ThreadTimerError {
    /// Joining of the timer thread failed.
    CouldNotJoinThread,
}

#[derive(Debug)]
struct ScheduledEntry<O> {
    deadline: SystemTime,
    generation: u64,
    state: O,
}

#[derive(Debug)]
struct HeapEntry<I> {
    deadline: SystemTime,
    generation: u64,
    seq: u64,
    id: I,
}

impl<I: Eq> PartialEq for HeapEntry<I> {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
            && self.generation == other.generation
            && self.seq == other.seq
    }
}

impl<I: Eq> Eq for HeapEntry<I> {}

impl<I: Eq> PartialOrd for HeapEntry<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: Eq> Ord for HeapEntry<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        match other.deadline.cmp(&self.deadline) {
            Ordering::Equal => other.seq.cmp(&self.seq),
            ord => ord,
        }
    }
}

struct TimerThread<I, O>
where
    I: Hash + Clone + Eq + fmt::Debug,
    O: State<Id = I> + fmt::Debug,
{
    entry_queue: BinaryHeap<HeapEntry<I>>,
    entries: FxHashMap<I, ScheduledEntry<O>>,
    work_queue: channel::Receiver<TimerMsg<I, O>>,
    running: bool,
    next_generation: u64,
    next_seq: u64,
}

impl<I, O> TimerThread<I, O>
where
    I: Hash + Clone + Eq + fmt::Debug,
    O: State<Id = I> + fmt::Debug,
{
    fn new(work_queue: channel::Receiver<TimerMsg<I, O>>) -> Self {
        TimerThread {
            entry_queue: BinaryHeap::new(),
            entries: FxHashMap::default(),
            work_queue,
            running: true,
            next_generation: 0,
            next_seq: 0,
        }
    }

    fn run_with_clock<C: Clock>(mut self, clock: C) {
        'run_loop: while self.running {
            self.process_due(clock.now());
            if !self.running {
                break 'run_loop;
            }

            let next_deadline = self.next_deadline();
            match next_deadline {
                None => match self.work_queue.recv() {
                    Ok(msg) => self.handle_msg(msg, &clock),
                    Err(channel::RecvError) => break 'run_loop,
                },
                Some(deadline) => {
                    let now = clock.now();
                    if deadline <= now {
                        continue 'run_loop;
                    }
                    let wait = deadline
                        .duration_since(now)
                        .unwrap_or(Duration::from_millis(0));
                    match self.work_queue.recv_timeout(wait) {
                        Ok(msg) => self.handle_msg(msg, &clock),
                        Err(channel::RecvTimeoutError::Timeout) => (),
                        Err(channel::RecvTimeoutError::Disconnected) => break 'run_loop,
                    }
                }
            }
        }
    }

    fn handle_msg<C: Clock>(&mut self, msg: TimerMsg<I, O>, clock: &C) {
        match msg {
            TimerMsg::Stop => self.running = false,
            TimerMsg::Schedule(entry) => self.schedule_entry(entry, clock),
            TimerMsg::Cancel(id) => {
                self.entries.remove(&id);
            }
        }
    }

    fn schedule_entry<C: Clock>(&mut self, entry: WallClockTimerEntry<I, O>, clock: &C) {
        let now = clock.now();
        if entry.deadline <= now {
            entry.state.trigger();
            return;
        }
        let id = entry.id().clone();
        self.insert_entry(id, entry.deadline, entry.state);
    }

    fn insert_entry(&mut self, id: I, deadline: SystemTime, state: O) {
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1);
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        self.entries.insert(
            id.clone(),
            ScheduledEntry {
                deadline,
                generation,
                state,
            },
        );
        self.entry_queue.push(HeapEntry {
            deadline,
            generation,
            seq,
            id,
        });
    }

    fn process_due(&mut self, now: SystemTime) {
        while let Some(scheduled) = self.pop_next_due(now) {
            scheduled.state.trigger();
        }
    }

    fn next_deadline(&mut self) -> Option<SystemTime> {
        while let Some(top) = self.entry_queue.peek() {
            let valid = self
                .entries
                .get(&top.id)
                .map(|entry| entry.generation == top.generation && entry.deadline == top.deadline)
                .unwrap_or(false);
            if valid {
                return Some(top.deadline);
            }
            self.entry_queue.pop();
        }
        None
    }

    fn pop_next_due(&mut self, now: SystemTime) -> Option<ScheduledEntry<O>> {
        while let Some(top) = self.entry_queue.peek() {
            let valid = self
                .entries
                .get(&top.id)
                .map(|entry| entry.generation == top.generation && entry.deadline == top.deadline)
                .unwrap_or(false);
            if valid {
                if top.deadline > now {
                    return None;
                }
                let entry = self.entry_queue.pop().expect("peeked entry");
                let scheduled = self.entries.remove(&entry.id).expect("entry should exist");
                return Some(scheduled);
            }
            self.entry_queue.pop();
        }
        None
    }

    #[cfg(test)]
    fn step<C: Clock>(&mut self, clock: &C) {
        self.process_due(clock.now());
        'recv_loop: loop {
            match self.work_queue.try_recv() {
                Ok(msg) => self.handle_msg(msg, clock),
                Err(channel::TryRecvError::Empty) => break 'recv_loop,
                Err(channel::TryRecvError::Disconnected) => {
                    self.running = false;
                    break 'recv_loop;
                }
            }
        }
        self.process_due(clock.now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timers::ClosureTimer;
    use std::sync::{
        Arc,
        Mutex,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    };

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
        let (s, r) = channel::unbounded();
        let mut timer = TimerThread::<u64, TestState>::new(r);
        let clock = MockClock::new(SystemTime::UNIX_EPOCH);
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = Arc::new(AtomicUsize::new(0));

        let entry = WallClockTimerEntry {
            deadline: SystemTime::UNIX_EPOCH + Duration::from_secs(5),
            state: TestState {
                id: 1,
                hits: hits.clone(),
            },
        };
        let later_entry = WallClockTimerEntry {
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
        let timer = TimerWithThread::<u64, crate::timers::ClosureState<u64>>::new().expect("timer");
        let mut tref = timer.timer_ref();

        let far_deadline = SystemTime::now() + Duration::from_secs(60);
        tref.schedule_action_at(1, far_deadline, |_| {});

        let hits = Arc::new(AtomicUsize::new(0));
        let hits_clone = hits.clone();
        let near_deadline = SystemTime::now() + Duration::from_millis(50);
        tref.schedule_action_at(2, near_deadline, move |_| {
            hits_clone.fetch_add(1, AtomicOrdering::SeqCst);
        });

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
        );
        let hits2_clone = hits2.clone();
        tref.schedule_action_at(
            2,
            SystemTime::UNIX_EPOCH + Duration::from_secs(50),
            move |_| {
                hits2_clone.fetch_add(1, AtomicOrdering::SeqCst);
            },
        );

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
}
