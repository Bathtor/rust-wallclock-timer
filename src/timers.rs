use std::{fmt, hash::Hash};

/// A trait for state that can be triggered once.
pub trait State {
    /// The type of the unique id of the outstanding timeout.
    type Id: Hash + Clone + Eq;

    /// A reference to the id associated with this state.
    fn id(&self) -> &Self::Id;
    /// Trigger should be called by the timer implementation
    /// when the timeout has expired.
    fn trigger(self);
}

/// A concrete entry for an outstanding timeout using a wall-clock deadline.
#[derive(Debug)]
pub struct WallClockTimerEntry<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    /// The wall clock deadline at which this should trigger.
    pub deadline: std::time::SystemTime,
    /// The information to store along with the timer.
    pub state: O,
}

impl<I, O> WallClockTimerEntry<I, O>
where
    I: Hash + Clone + Eq,
    O: State<Id = I>,
{
    /// A reference to the id associated with this entry.
    pub fn id(&self) -> &I {
        self.state.id()
    }
}

/// A low-level wall-clock timer API.
pub trait WallClockTimer {
    /// A type to uniquely identify any timeout to be scheduled or cancelled.
    type Id: Hash + Clone + Eq;
    /// The type of state to keep for oneshot timers.
    type State: State<Id = Self::Id>;

    /// Schedule the `state` to be triggered at the given wall-clock `deadline`.
    fn schedule_at(&mut self, deadline: std::time::SystemTime, state: Self::State);

    /// Cancel the timer indicated by the unique `id`.
    fn cancel(&mut self, id: &Self::Id);
}

/// A timeout state for a timer using a closure as the triggering action.
pub struct ClosureState<I> {
    id: I,
    action: Box<dyn FnOnce(I) + Send + 'static>,
}

impl<I> ClosureState<I> {
    /// Produces a new instance of this state type
    /// from a unique id and the action to be executed when it expires.
    pub fn new<F>(id: I, action: F) -> Self
    where
        F: FnOnce(I) + Send + 'static,
    {
        ClosureState {
            id,
            action: Box::new(action),
        }
    }
}

impl<I> State for ClosureState<I>
where
    I: Hash + Clone + Eq,
{
    type Id = I;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn trigger(self) {
        (self.action)(self.id)
    }
}

impl<I> fmt::Debug for ClosureState<I>
where
    I: Hash + Clone + Eq + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OneShotClosureState(id={:?}, action=<function>)",
            self.id
        )
    }
}

/// Convenience API for timers that use the closure state types.
pub trait ClosureTimer: WallClockTimer {
    /// Schedule `action` to be executed at `deadline`.
    fn schedule_action_at<F>(&mut self, id: Self::Id, deadline: std::time::SystemTime, action: F)
    where
        F: FnOnce(Self::Id) + Send + 'static;
}

impl<I, T> ClosureTimer for T
where
    I: Hash + Clone + Eq,
    T: WallClockTimer<Id = I, State = ClosureState<I>>,
{
    fn schedule_action_at<F>(&mut self, id: Self::Id, deadline: std::time::SystemTime, action: F)
    where
        F: FnOnce(Self::Id) + Send + 'static,
    {
        self.schedule_at(deadline, ClosureState::new(id, action))
    }
}
