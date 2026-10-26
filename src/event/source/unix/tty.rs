#[cfg(feature = "libc")]
use std::os::unix::prelude::AsRawFd;
use std::{
    collections::VecDeque,
    io,
    os::unix::net::UnixStream,
    time::{Duration, Instant},
};

#[cfg(not(feature = "libc"))]
use rustix::fd::{AsFd, AsRawFd};

use signal_hook::low_level::pipe;

use crate::event::timeout::PollTimeout;
use crate::event::Event;
use filedescriptor::{poll, pollfd, POLLIN};

#[cfg(feature = "event-stream")]
use crate::event::sys::Waker;
use crate::event::{
    escape_wait, source::EventSource, sys::unix::parse::parse_event, InternalEvent, KeyCode,
};
use crate::terminal::sys::file_descriptor::{tty_fd, FileDesc};

/// Holds a prototypical Waker and a receiver we can wait on when doing select().
#[cfg(feature = "event-stream")]
struct WakePipe {
    receiver: UnixStream,
    waker: Waker,
}

#[cfg(feature = "event-stream")]
impl WakePipe {
    fn new() -> io::Result<Self> {
        let (receiver, sender) = nonblocking_unix_pair()?;
        Ok(WakePipe {
            receiver,
            waker: Waker::new(sender),
        })
    }
}

// I (@zrzka) wasn't able to read more than 1_022 bytes when testing
// reading on macOS/Linux -> we don't need bigger buffer and 1k of bytes
// is enough.
const TTY_BUFFER_SIZE: usize = 1_024;

pub(crate) struct UnixInternalEventSource {
    parser: Parser,
    tty_buffer: [u8; TTY_BUFFER_SIZE],
    tty: FileDesc<'static>,
    winch_signal_receiver: UnixStream,
    #[cfg(feature = "event-stream")]
    wake_pipe: WakePipe,
}

fn nonblocking_unix_pair() -> io::Result<(UnixStream, UnixStream)> {
    let (receiver, sender) = UnixStream::pair()?;
    receiver.set_nonblocking(true)?;
    sender.set_nonblocking(true)?;
    Ok((receiver, sender))
}

impl UnixInternalEventSource {
    pub fn new() -> io::Result<Self> {
        UnixInternalEventSource::from_file_descriptor(tty_fd()?)
    }

    pub(crate) fn from_file_descriptor(input_fd: FileDesc<'static>) -> io::Result<Self> {
        Ok(UnixInternalEventSource {
            parser: Parser::default(),
            tty_buffer: [0u8; TTY_BUFFER_SIZE],
            tty: input_fd,
            winch_signal_receiver: {
                let (receiver, sender) = nonblocking_unix_pair()?;
                // Unregistering is unnecessary because EventSource is a singleton
                #[cfg(feature = "libc")]
                pipe::register(libc::SIGWINCH, sender)?;
                #[cfg(not(feature = "libc"))]
                pipe::register(rustix::process::Signal::WINCH.as_raw(), sender)?;
                receiver
            },
            #[cfg(feature = "event-stream")]
            wake_pipe: WakePipe::new()?,
        })
    }
}

/// read_complete reads from a non-blocking file descriptor
/// until the buffer is full or it would block.
///
/// Similar to `std::io::Read::read_to_end`, except this function
/// only fills the given buffer and does not read beyond that.
fn read_complete(fd: &FileDesc, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        match fd.read(buf) {
            Ok(x) => return Ok(x),
            Err(e) => match e.kind() {
                io::ErrorKind::WouldBlock => return Ok(0),
                io::ErrorKind::Interrupted => continue,
                _ => return Err(e),
            },
        }
    }
}

impl EventSource for UnixInternalEventSource {
    fn try_read(&mut self, timeout: Option<Duration>) -> io::Result<Option<InternalEvent>> {
        let timeout = PollTimeout::new(timeout);
        let wait = escape_wait();

        fn make_pollfd<F: AsRawFd>(fd: &F) -> pollfd {
            pollfd {
                fd: fd.as_raw_fd(),
                events: POLLIN,
                revents: 0,
            }
        }

        #[cfg(not(feature = "event-stream"))]
        let mut fds = [
            make_pollfd(&self.tty),
            make_pollfd(&self.winch_signal_receiver),
        ];

        #[cfg(feature = "event-stream")]
        let mut fds = [
            make_pollfd(&self.tty),
            make_pollfd(&self.winch_signal_receiver),
            make_pollfd(&self.wake_pipe.receiver),
        ];

        // a lone ESC parked by an earlier call whose wait has passed
        if self.parser.flush_escape(wait, Instant::now()) {
            return Ok(self.parser.next());
        }

        while timeout.leftover().map_or(true, |t| !t.is_zero()) {
            // check if there are buffered events from the last read
            if let Some(event) = self.parser.next() {
                return Ok(Some(event));
            }
            let left = sooner(
                timeout.leftover(),
                self.parser.escape_left(wait, Instant::now()),
            );
            match poll(&mut fds, left) {
                Err(filedescriptor::Error::Poll(e)) | Err(filedescriptor::Error::Io(e)) => {
                    match e.kind() {
                        // retry on EINTR
                        io::ErrorKind::Interrupted => continue,
                        _ => return Err(e),
                    }
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("got unexpected error while polling: {:?}", e),
                    ))
                }
                Ok(_) => (),
            };
            if fds[0].revents & POLLIN != 0 {
                loop {
                    let read_count = read_complete(&self.tty, &mut self.tty_buffer)?;
                    if read_count > 0 {
                        self.parser.advance(
                            &self.tty_buffer[..read_count],
                            read_count == TTY_BUFFER_SIZE,
                            wait,
                            Instant::now(),
                        );
                    }

                    if let Some(event) = self.parser.next() {
                        return Ok(Some(event));
                    }

                    if read_count == 0 {
                        break;
                    }
                }
            } else if self.parser.flush_escape(wait, Instant::now()) {
                return Ok(self.parser.next());
            }
            if fds[1].revents & POLLIN != 0 {
                #[cfg(feature = "libc")]
                let fd = FileDesc::new(self.winch_signal_receiver.as_raw_fd(), false);
                #[cfg(not(feature = "libc"))]
                let fd = FileDesc::Borrowed(self.winch_signal_receiver.as_fd());
                // drain the pipe
                while read_complete(&fd, &mut [0; 1024])? != 0 {}
                // TODO Should we remove tput?
                //
                // This can take a really long time, because terminal::size can
                // launch new process (tput) and then it parses its output. It's
                // not a really long time from the absolute time point of view, but
                // it's a really long time from the mio, async-std/tokio executor, ...
                // point of view.
                let new_size = crate::terminal::size()?;
                return Ok(Some(InternalEvent::Event(Event::Resize(
                    new_size.0, new_size.1,
                ))));
            }

            #[cfg(feature = "event-stream")]
            if fds[2].revents & POLLIN != 0 {
                #[cfg(feature = "libc")]
                let fd = FileDesc::new(self.wake_pipe.receiver.as_raw_fd(), false);
                #[cfg(not(feature = "libc"))]
                let fd = FileDesc::Borrowed(self.wake_pipe.receiver.as_fd());
                // drain the pipe
                while read_complete(&fd, &mut [0; 1024])? != 0 {}

                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "Poll operation was woken up by `Waker::wake`",
                ));
            }
        }
        Ok(None)
    }

    #[cfg(feature = "event-stream")]
    fn waker(&self) -> Waker {
        self.wake_pipe.waker.clone()
    }
}

//
// Following `Parser` structure exists for two reasons:
//
//  * mimic anes Parser interface
//  * move the advancing, parsing, ... stuff out of the `try_read` method
//
#[derive(Debug)]
struct Parser {
    buffer: Vec<u8>,
    internal_events: VecDeque<InternalEvent>,
    // When the buffer holds a lone ESC that ended a read and waits, for the
    // `escape_wait` in force, for the rest of its sequence.
    parked: Option<Instant>,
}

impl Default for Parser {
    fn default() -> Self {
        Parser {
            // This buffer is used for -> 1 <- ANSI escape sequence. Are we
            // aware of any ANSI escape sequence that is bigger? Can we make
            // it smaller?
            //
            // Probably not worth spending more time on this as "there's a plan"
            // to use the anes crate parser.
            buffer: Vec::with_capacity(256),
            // TTY_BUFFER_SIZE is 1_024 bytes. How many ANSI escape sequences can
            // fit? What is an average sequence length? Let's guess here
            // and say that the average ANSI escape sequence length is 8 bytes. Thus
            // the buffer size should be 1024/8=128 to avoid additional allocations
            // when processing large amounts of data.
            //
            // There's no need to make it bigger, because when you look at the `try_read`
            // method implementation, all events are consumed before the next TTY_BUFFER
            // is processed -> events pushed.
            internal_events: VecDeque::with_capacity(128),
            parked: None,
        }
    }
}

impl Parser {
    // With a nonzero `wait`, a lone ESC that ends `buffer` with no more input
    // pending is held, from `now`, for the rest of its sequence instead of
    // being read as Esc.
    fn advance(&mut self, buffer: &[u8], more: bool, wait: Duration, now: Instant) {
        // a parked ESC followed by another ESC was the Esc key
        if self.parked.take().is_some() && buffer.first() == Some(&b'\x1B') {
            self.buffer.clear();
            self.push_escape();
        }
        for (idx, byte) in buffer.iter().enumerate() {
            let more = idx + 1 < buffer.len() || more;

            self.buffer.push(*byte);

            let park = !more && !wait.is_zero() && self.buffer == *b"\x1B";
            if park {
                self.parked = Some(now);
            }

            match parse_event(&self.buffer, more || park) {
                Ok(Some(ie)) => {
                    self.internal_events.push_back(ie);
                    self.buffer.clear();
                }
                Ok(None) => {
                    // Event can't be parsed, because we don't have enough bytes for
                    // the current sequence. Keep the buffer and process next bytes.
                }
                Err(_) => {
                    // Event can't be parsed (not enough parameters, parameter is not a number, ...).
                    // Clear the buffer and continue with another sequence.
                    self.buffer.clear();
                }
            }
        }
    }

    // How long a parked ESC still waits at `now`, or None when none is parked.
    fn escape_left(&self, wait: Duration, now: Instant) -> Option<Duration> {
        self.parked
            .map(|at| (at + wait).saturating_duration_since(now))
    }

    // Reads a parked ESC as Esc once its wait has passed at `now`, and says
    // whether it did.
    fn flush_escape(&mut self, wait: Duration, now: Instant) -> bool {
        if self.escape_left(wait, now) != Some(Duration::ZERO) {
            return false;
        }
        self.parked = None;
        self.buffer.clear();
        self.push_escape();
        true
    }

    fn push_escape(&mut self) {
        self.internal_events
            .push_back(InternalEvent::Event(Event::Key(KeyCode::Esc.into())));
    }
}

// The sooner of two poll timeouts, None being no timeout.
fn sooner(a: Option<Duration>, b: Option<Duration>) -> Option<Duration> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) => a,
        (None, b) => b,
    }
}

impl Iterator for Parser {
    type Item = InternalEvent;

    fn next(&mut self) -> Option<Self::Item> {
        self.internal_events.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        os::unix::net::UnixStream,
        time::{Duration, Instant},
    };

    use super::{Parser, UnixInternalEventSource};
    use crate::event::{
        set_escape_wait, source::EventSource, Event, InternalEvent, KeyCode, KeyEvent,
        KeyModifiers, MouseEvent, MouseEventKind,
    };
    use crate::terminal::sys::file_descriptor::FileDesc;

    const WAIT: Duration = Duration::from_millis(30);

    fn esc() -> InternalEvent {
        InternalEvent::Event(Event::Key(KeyCode::Esc.into()))
    }

    // what `\x1b[<64;10;5M` reports: one wheel step up at column 10, row 5
    fn wheel_up() -> InternalEvent {
        InternalEvent::Event(Event::Mouse(MouseEvent {
            kind: MouseEventKind::ScrollUp,
            column: 9,
            row: 4,
            modifiers: KeyModifiers::NONE,
        }))
    }

    #[test]
    fn mouse_report_split_after_its_escape_is_one_mouse_event() {
        let mut parser = Parser::default();
        let t0 = Instant::now();
        parser.advance(b"\x1b", false, WAIT, t0);
        assert_eq!(parser.next(), None);
        assert_eq!(parser.escape_left(WAIT, t0), Some(WAIT));

        parser.advance(b"[<64;10;5M", false, WAIT, t0 + Duration::from_millis(1));
        assert_eq!(parser.by_ref().collect::<Vec<_>>(), vec![wheel_up()]);
        assert_eq!(parser.escape_left(WAIT, t0), None);
    }

    #[test]
    fn escape_ending_a_read_after_a_report_is_parked() {
        let mut parser = Parser::default();
        let t0 = Instant::now();
        parser.advance(b"\x1b[<64;10;5M\x1b", false, WAIT, t0);
        assert_eq!(parser.by_ref().collect::<Vec<_>>(), vec![wheel_up()]);
        assert_eq!(parser.escape_left(WAIT, t0), Some(WAIT));
    }

    #[test]
    fn parked_escape_is_esc_once_its_wait_passes() {
        let mut parser = Parser::default();
        let t0 = Instant::now();
        parser.advance(b"\x1b", false, WAIT, t0);
        assert!(!parser.flush_escape(WAIT, t0 + WAIT - Duration::from_millis(1)));
        assert_eq!(parser.next(), None);
        assert!(parser.flush_escape(WAIT, t0 + WAIT));
        assert_eq!(parser.by_ref().collect::<Vec<_>>(), vec![esc()]);
        assert_eq!(parser.escape_left(WAIT, t0 + WAIT), None);
    }

    #[test]
    fn no_wait_reads_a_lone_escape_as_esc_at_once() {
        let mut parser = Parser::default();
        let t0 = Instant::now();
        parser.advance(b"\x1b", false, Duration::ZERO, t0);
        assert_eq!(parser.by_ref().collect::<Vec<_>>(), vec![esc()]);
        assert_eq!(parser.escape_left(Duration::ZERO, t0), None);
    }

    #[test]
    fn parked_escape_then_a_letter_is_alt_and_the_letter() {
        let mut parser = Parser::default();
        let t0 = Instant::now();
        parser.advance(b"\x1b", false, WAIT, t0);
        parser.advance(b"x", false, WAIT, t0);
        assert_eq!(
            parser.by_ref().collect::<Vec<_>>(),
            vec![InternalEvent::Event(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::ALT
            )))]
        );
    }

    #[test]
    fn parked_escape_then_another_escape_is_two_esc() {
        let mut parser = Parser::default();
        let t0 = Instant::now();
        parser.advance(b"\x1b", false, WAIT, t0);
        parser.advance(b"\x1b", false, WAIT, t0);
        assert_eq!(parser.by_ref().collect::<Vec<_>>(), vec![esc()]);
        assert!(parser.flush_escape(WAIT, t0 + WAIT));
        assert_eq!(parser.by_ref().collect::<Vec<_>>(), vec![esc()]);
    }

    // The one test that sets the process-wide wait: no other test reads it.
    #[test]
    fn try_read_waits_for_the_rest_of_a_split_report() {
        let (input, mut terminal) = UnixStream::pair().unwrap();
        input.set_nonblocking(true).unwrap();
        #[cfg(feature = "libc")]
        let fd = FileDesc::new(std::os::unix::io::IntoRawFd::into_raw_fd(input), true);
        #[cfg(not(feature = "libc"))]
        let fd = FileDesc::Owned(input.into());
        let mut source = UnixInternalEventSource::from_file_descriptor(fd).unwrap();

        // longer than any stall, so the split is never read as Esc
        set_escape_wait(Duration::from_secs(60));
        terminal.write_all(b"\x1b").unwrap();
        assert_eq!(
            source.try_read(Some(Duration::from_millis(10))).unwrap(),
            None
        );
        terminal.write_all(b"[<64;10;5M").unwrap();
        assert_eq!(source.try_read(None).unwrap(), Some(wheel_up()));

        // with nothing after it, the ESC is Esc once the wait passes
        set_escape_wait(Duration::from_millis(20));
        terminal.write_all(b"\x1b").unwrap();
        assert_eq!(source.try_read(None).unwrap(), Some(esc()));
        set_escape_wait(Duration::ZERO);
    }
}
