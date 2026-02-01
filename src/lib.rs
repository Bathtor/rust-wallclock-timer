//! Wall-clock-based timer utilities.
//!
//! This crate uses the `log` facade for internal error reporting. Provide a logger
//! implementation in your application (e.g. `simplelogger`, `env_logger`) to see logs.

pub mod thread_timer;
mod timers;

pub use timers::*;
