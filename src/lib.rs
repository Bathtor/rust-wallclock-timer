#![cfg_attr(docsrs, feature(doc_cfg))]
//! Wall-clock-based timer utilities.
//!
//! This crate provides a timer that triggers actions at fixed wall-clock deadlines,
//! running on a dedicated thread. You interact with the timer through a cloneable
//! handle (`TimerRef`) and schedule actions by absolute `SystemTime` deadlines.
//!
//! # Quick start (UUID ids)
//! ```no_run
//! use rust_wallclock_timer::thread_timer::UuidClosureTimer;
//! use rust_wallclock_timer::ClosureTimer;
//! use std::time::{Duration, SystemTime};
//! use uuid::Uuid;
//!
//! let timer = UuidClosureTimer::default();
//! let mut tref = timer.timer_ref();
//!
//! let deadline = SystemTime::now() + Duration::from_secs(1);
//! tref.schedule_action_at(Uuid::new_v4(), deadline, |id| {
//!     println!("Triggered {id}");
//! }).expect("schedule");
//!
//! std::thread::sleep(Duration::from_secs(2));
//! timer.shutdown().expect("shutdown");
//! ```
//!
//! # Errors and logging
//! Public APIs return  an implementation-specific [[Result]]
//! (e.g. [[thread_timer::ThreadTimerError]] for the [[thread_timer::TimerWithThread]])
//! for failures (such as channel send errors).
//!
//! The crate also uses the `log` facade for internal error reporting. Provide a logger
//! implementation in your application to see logs.
//!
//! # UUID feature
//! Enable the `uuid` feature to use UUID ids and the UUID-based shorthand aliases:
//! - [[thread_timer::UuidClosureTimer]]
//! - [[thread_timer::UuidClosureTimerRef]]
//!
//! # Chrono feature
//! Enable the `chrono` feature to schedule timers using `chrono::DateTime` via
//! [[timers::chrono::ChronoTimer::schedule_at_datetime]].

pub mod thread_timer;
mod timers;

pub use timers::*;
