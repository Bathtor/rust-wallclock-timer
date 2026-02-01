# Wall-clock Timer
A wall-clock-based timer utility for Rust.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/Bathtor/rrust-wallclock-timer)
[![Cargo](https://img.shields.io/crates/v/wallclock-timer?label=Cargo&style=plastic)](https://crates.io/crates/wallclock-timer) 
[![Documentation](https://docs.rs/wallclock-timer/badge.svg)](https://docs.rs/wallclock-timer)
[![Build Status](https://github.com/Bathtor/rust-wallclock-timer/actions/workflows/ci.yml/badge.svg)](https://github.com/Bathtor/rust-hash-wheel-timer/actions/workflows/ci.yml)

## Provided APIs

### Thread Timer
The `thread_timer` module provides a timer for real-time event scheduling against the system's time.
It runs on its own dedicated thread and uses a shareable handle called a `TimerRef` for communication with other threads.

## Documentation

For reference and examples check the [API Docs](https://docs.rs/wallclock-timer).

## License

Licensed under the terms of MIT license.

See [LICENSE](LICENSE) for details.
