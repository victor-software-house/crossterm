#[cfg(all(unix, feature = "events"))]
pub(crate) use unix::waker::Waker;
#[cfg(all(windows, feature = "events"))]
pub(crate) use windows::waker::Waker;

#[cfg(unix)]
pub(crate) mod unix;
#[cfg(windows)]
pub(crate) mod windows;
