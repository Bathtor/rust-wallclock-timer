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
        let (s, r) = channel::unbounded();
        let handle = thread::Builder::new()
            .name("wallclock-timer-thread".to_string())
            .spawn(move || {
                let timer = TimerThread::new(r);
                timer.run();
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

    fn run(mut self) {
        while self.running {
            self.process_due();
            if !self.running {
                break;
            }

            let next_deadline = self.next_deadline();
            match next_deadline {
                None => match self.work_queue.recv() {
                    Ok(msg) => self.handle_msg(msg),
                    Err(channel::RecvError) => break,
                },
                Some(deadline) => {
                    let now = SystemTime::now();
                    if deadline <= now {
                        continue;
                    }
                    let wait = deadline
                        .duration_since(now)
                        .unwrap_or(Duration::from_millis(0));
                    match self.work_queue.recv_timeout(wait) {
                        Ok(msg) => self.handle_msg(msg),
                        Err(channel::RecvTimeoutError::Timeout) => (),
                        Err(channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
            }
        }
    }

    fn handle_msg(&mut self, msg: TimerMsg<I, O>) {
        match msg {
            TimerMsg::Stop => self.running = false,
            TimerMsg::Schedule(entry) => self.schedule_entry(entry),
            TimerMsg::Cancel(id) => {
                self.entries.remove(&id);
            }
        }
    }

    fn schedule_entry(&mut self, entry: WallClockTimerEntry<I, O>) {
        let now = SystemTime::now();
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

    fn process_due(&mut self) {
        let now = SystemTime::now();
        loop {
            let due = self.pop_next_due(now);
            let Some(scheduled) = due else {
                break;
            };
            scheduled.state.trigger();
        }
    }

    fn next_deadline(&mut self) -> Option<SystemTime> {
        loop {
            let top = self.entry_queue.peek()?;
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
    }

    fn pop_next_due(&mut self, now: SystemTime) -> Option<ScheduledEntry<O>> {
        loop {
            let top = self.entry_queue.peek()?;
            let valid = self
                .entries
                .get(&top.id)
                .map(|entry| entry.generation == top.generation && entry.deadline == top.deadline)
                .unwrap_or(false);
            if !valid {
                self.entry_queue.pop();
                continue;
            }
            if top.deadline > now {
                return None;
            }
            let entry = self.entry_queue.pop().expect("peeked entry");
            let scheduled = self.entries.remove(&entry.id).expect("entry should exist");
            return Some(scheduled);
        }
    }
}
