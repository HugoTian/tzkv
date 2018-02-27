// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Deref;
use std::ops::DerefMut;
use std::{slice, thread};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use std::collections::hash_map::Entry;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::vec_deque::{Iter, VecDeque};
use std::{io, u64};

use prometheus;
use rand::{self, ThreadRng};
use protobuf::Message;

#[macro_use]
pub mod macros;
pub mod logger;
pub mod panic_hook;
pub mod worker;
pub mod codec;
pub mod rocksdb;
pub mod config;
pub mod buf;
pub mod transport;
pub mod file;
pub mod file_log;
pub mod metrics;
pub mod threadpool;
pub mod collections;
pub mod time;
pub mod io_limiter;
pub mod security;
pub mod timer;
pub mod sys;
pub mod futurepool;

pub use self::rocksdb::properties;

#[cfg(target_os = "linux")]
mod thread_metrics;

pub const NO_LIMIT: u64 = u64::MAX;

pub trait AssertSend: Send {}

pub trait AssertSync: Sync {}

pub fn limit_size<T: Message + Clone>(entries: &mut Vec<T>, max: u64) {
    if max == NO_LIMIT || entries.len() <= 1 {
        return;
    }

    let mut size = 0;
    let limit = entries
        .iter()
        .take_while(|&e| {
            if size == 0 {
                size += u64::from(Message::compute_size(e));
                true
            } else {
                size += u64::from(Message::compute_size(e));
                size <= max
            }
        })
        .count();

    entries.truncate(limit);
}

/// Take slices in the range.
///
/// ### Panic
///
/// if [low, high) is out of bound.
pub fn slices_in_range<T>(entry: &VecDeque<T>, low: usize, high: usize) -> (&[T], &[T]) {
    let (first, second) = entry.as_slices();
    if low >= first.len() {
        (&second[low - first.len()..high - first.len()], &[])
    } else if high <= first.len() {
        (&first[low..high], &[])
    } else {
        (&first[low..], &second[..high - first.len()])
    }
}

pub struct DefaultRng {
    rng: ThreadRng,
}

impl DefaultRng {
    fn new() -> DefaultRng {
        DefaultRng {
            rng: rand::thread_rng(),
        }
    }
}

impl Default for DefaultRng {
    fn default() -> DefaultRng {
        DefaultRng::new()
    }
}

impl Deref for DefaultRng {
    type Target = ThreadRng;

    fn deref(&self) -> &ThreadRng {
        &self.rng
    }
}

impl DerefMut for DefaultRng {
    fn deref_mut(&mut self) -> &mut ThreadRng {
        &mut self.rng
    }
}

/// A handy shortcut to replace `RwLock` write/read().unwrap() pattern to
/// shortcut wl and rl.
pub trait HandyRwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<T>;
    fn rl(&self) -> RwLockReadGuard<T>;
}

impl<T> HandyRwLock<T> for RwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<T> {
        self.write().unwrap()
    }

    fn rl(&self) -> RwLockReadGuard<T> {
        self.read().unwrap()
    }
}

// A helper function to parse SocketAddr for mio.
// In mio example, it uses "127.0.0.1:80".parse() to get the SocketAddr,
// but it is just ok for "ip:port", not "host:port".
pub fn to_socket_addr<A: ToSocketAddrs>(addr: A) -> io::Result<SocketAddr> {
    let addrs = addr.to_socket_addrs()?;
    Ok(addrs.collect::<Vec<SocketAddr>>()[0])
}

/// A function to escape a byte array to a readable ascii string.
/// escape rules follow golang/protobuf.
/// <https://github.com/golang/protobuf/blob/master/proto/text.go#L578>
///
/// # Examples
///
/// ```
/// use tikv::util::escape;
///
/// assert_eq!(r"ab", escape(b"ab"));
/// assert_eq!(r"a\\023", escape(b"a\\023"));
/// assert_eq!(r"a\000", escape(b"a\0"));
/// assert_eq!("a\\r\\n\\t '\\\"\\\\", escape(b"a\r\n\t '\"\\"));
/// assert_eq!(r"\342\235\244\360\237\220\267", escape("❤🐷".as_bytes()));
/// ```
pub fn escape(data: &[u8]) -> String {
    let mut escaped = Vec::with_capacity(data.len() * 4);
    for &c in data {
        match c {
            b'\n' => escaped.extend_from_slice(br"\n"),
            b'\r' => escaped.extend_from_slice(br"\r"),
            b'\t' => escaped.extend_from_slice(br"\t"),
            b'"' => escaped.extend_from_slice(b"\\\""),
            b'\\' => escaped.extend_from_slice(br"\\"),
            _ => {
                if c >= 0x20 && c < 0x7f {
                    // c is printable
                    escaped.push(c);
                } else {
                    escaped.push(b'\\');
                    escaped.push(b'0' + (c >> 6));
                    escaped.push(b'0' + ((c >> 3) & 7));
                    escaped.push(b'0' + (c & 7));
                }
            }
        }
    }
    escaped.shrink_to_fit();
    unsafe { String::from_utf8_unchecked(escaped) }
}

/// A function to unescape an escaped string to a byte array.
///
/// # Panic
///
/// If s is not a properly encoded string.
///
/// # Examples
///
/// ```
/// use tikv::util::unescape;
///
/// assert_eq!(unescape(r"ab"), b"ab");
/// assert_eq!(unescape(r"a\\023"), b"a\\023");
/// assert_eq!(unescape(r"a\000"), b"a\0");
/// assert_eq!(unescape("a\\r\\n\\t '\\\"\\\\"), b"a\r\n\t '\"\\");
/// assert_eq!(unescape(r"\342\235\244\360\237\220\267"), "❤🐷".as_bytes());
/// ```
///
pub fn unescape(s: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(s.len());
    let mut bytes = s.bytes();
    loop {
        let b = match bytes.next() {
            None => break,
            Some(t) => t,
        };
        if b != b'\\' {
            buf.push(b);
            continue;
        }
        match bytes.next().unwrap() {
            b'"' => buf.push(b'"'),
            b'\'' => buf.push(b'\''),
            b'\\' => buf.push(b'\\'),
            b'n' => buf.push(b'\n'),
            b't' => buf.push(b'\t'),
            b'r' => buf.push(b'\r'),
            b => {
                let b1 = b - b'0';
                let b2 = bytes.next().unwrap() - b'0';
                let b3 = bytes.next().unwrap() - b'0';
                buf.push((b1 << 6) + (b2 << 3) + b3);
            }
        }
    }
    buf.shrink_to_fit();
    buf
}

/// Convert a borrow to a slice.
pub fn as_slice<T>(t: &T) -> &[T] {
    unsafe {
        let ptr = t as *const T;
        slice::from_raw_parts(ptr, 1)
    }
}

/// `TryInsertWith` is a helper trait for `Entry` to accept a failable closure.
pub trait TryInsertWith<'a, V, E> {
    fn or_try_insert_with<F: FnOnce() -> Result<V, E>>(self, default: F) -> Result<&'a mut V, E>;
}

impl<'a, T: 'a, V: 'a, E> TryInsertWith<'a, V, E> for Entry<'a, T, V> {
    fn or_try_insert_with<F: FnOnce() -> Result<V, E>>(self, default: F) -> Result<&'a mut V, E> {
        match self {
            Entry::Occupied(e) => Ok(e.into_mut()),
            Entry::Vacant(e) => {
                let v = default()?;
                Ok(e.insert(v))
            }
        }
    }
}

pub fn get_tag_from_thread_name() -> Option<String> {
    thread::current()
        .name()
        .and_then(|name| name.split("::").skip(1).last())
        .map(From::from)
}

/// `DeferContext` will invoke the wrapped closure when dropped.
pub struct DeferContext<T: FnOnce()> {
    t: Option<T>,
}

impl<T: FnOnce()> DeferContext<T> {
    pub fn new(t: T) -> DeferContext<T> {
        DeferContext { t: Some(t) }
    }
}

impl<T: FnOnce()> Drop for DeferContext<T> {
    fn drop(&mut self) {
        self.t.take().unwrap()()
    }
}

/// Represents a value of one of two possible types (a more generic Result.)
#[derive(Debug, Clone)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    #[inline]
    pub fn as_ref(&self) -> Either<&L, &R> {
        match *self {
            Either::Left(ref l) => Either::Left(l),
            Either::Right(ref r) => Either::Right(r),
        }
    }

    #[inline]
    pub fn left(self) -> Option<L> {
        match self {
            Either::Left(l) => Some(l),
            _ => None,
        }
    }

    #[inline]
    pub fn right(self) -> Option<R> {
        match self {
            Either::Right(r) => Some(r),
            _ => None,
        }
    }
}

/// `build_info` returns a tuple of Strings that contains build utc time and commit hash.
pub fn build_info() -> (String, String, String, String) {
    let raw = include_str!(concat!(env!("OUT_DIR"), "/build-info.txt"));
    let mut parts = raw.split('\n');

    (
        parts.next().unwrap_or("None").to_owned(),
        parts.next().unwrap_or("None").to_owned(),
        parts.next().unwrap_or("None").to_owned(),
        parts.next().unwrap_or("None").to_owned(),
    )
}

/// `print_tikv_info` prints the tikv version information to the standard output.
pub fn print_tikv_info() {
    let (hash, branch, date, rustc) = build_info();
    info!("Welcome to TiKV.");
    info!("Release Version:   {}", env!("CARGO_PKG_VERSION"));
    info!("Git Commit Hash:   {}", hash);
    info!("Git Commit Branch: {}", branch);
    info!("UTC Build Time:    {}", date);
    info!("Rustc Version:     {}", rustc);
}

/// `run_prometheus` runs a background prometheus client.
pub fn run_prometheus(
    interval: Duration,
    address: &str,
    job: &str,
) -> Option<thread::JoinHandle<()>> {
    if interval == Duration::from_secs(0) {
        return None;
    }

    let job = job.to_owned();
    let address = address.to_owned();
    let handler = thread::Builder::new()
        .name("promepusher".to_owned())
        .spawn(move || loop {
            let metric_familys = prometheus::gather();

            let res = prometheus::push_metrics(
                &job,
                prometheus::hostname_grouping_key(),
                &address,
                metric_familys,
            );
            if let Err(e) = res {
                error!("fail to push metrics: {}", e);
            }

            thread::sleep(interval);
        })
        .unwrap();

    Some(handler)
}

#[cfg(target_os = "linux")]
pub use self::thread_metrics::monitor_threads;

#[cfg(not(target_os = "linux"))]
pub fn monitor_threads<S: Into<String>>(_: S) -> io::Result<()> {
    Ok(())
}

/// A simple ring queue with fixed capacity.
pub struct RingQueue<T> {
    buf: VecDeque<T>,
    cap: usize,
}

impl<T> RingQueue<T> {
    #[inline]
    fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn with_capacity(cap: usize) -> RingQueue<T> {
        RingQueue {
            buf: VecDeque::with_capacity(cap),
            cap: cap,
        }
    }

    pub fn push(&mut self, t: T) {
        if self.len() == self.cap {
            self.buf.pop_front();
        }
        self.buf.push_back(t);
    }

    pub fn iter(&self) -> Iter<T> {
        self.buf.iter()
    }

    pub fn swap_remove_front<F>(&mut self, f: F) -> Option<T>
    where
        F: FnMut(&T) -> bool,
    {
        if let Some(pos) = self.buf.iter().position(f) {
            self.buf.swap_remove_front(pos)
        } else {
            None
        }
    }
}

// `cfs_diff' Returns a Vec of cf which is in `a' but not in `b'.
pub fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| b.iter().find(|y| y == x).is_none())
        .map(|x| *x)
        .collect()
}

#[inline]
pub fn is_even(n: usize) -> bool {
    n & 1 == 0
}

pub struct MustConsumeVec<T> {
    tag: &'static str,
    v: Vec<T>,
}

impl<T> MustConsumeVec<T> {
    #[inline]
    pub fn new(tag: &'static str) -> MustConsumeVec<T> {
        MustConsumeVec::with_capacity(tag, 0)
    }

    #[inline]
    pub fn with_capacity(tag: &'static str, cap: usize) -> MustConsumeVec<T> {
        MustConsumeVec {
            tag: tag,
            v: Vec::with_capacity(cap),
        }
    }
}

impl<T> Deref for MustConsumeVec<T> {
    type Target = Vec<T>;

    fn deref(&self) -> &Vec<T> {
        &self.v
    }
}

impl<T> DerefMut for MustConsumeVec<T> {
    fn deref_mut(&mut self) -> &mut Vec<T> {
        &mut self.v
    }
}

impl<T> Drop for MustConsumeVec<T> {
    fn drop(&mut self) {
        if !self.is_empty() {
            panic!("resource leak detected: {}.", self.tag);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::*;
    use std::net::{AddrParseError, SocketAddr};
    use std::rc::Rc;
    use std::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use kvproto::eraftpb::Entry;
    use protobuf::Message;
    use super::*;

    #[test]
    fn test_to_socket_addr() {
        let tbls = vec![
            ("", false),
            ("127.0.0.1", false),
            ("localhost", false),
            ("127.0.0.1:80", true),
            ("localhost:80", true),
        ];

        for (addr, ok) in tbls {
            assert_eq!(to_socket_addr(addr).is_ok(), ok);
        }

        let tbls = vec![("localhost:80", false), ("127.0.0.1:80", true)];

        for (addr, ok) in tbls {
            let ret: Result<SocketAddr, AddrParseError> = addr.parse();
            assert_eq!(ret.is_ok(), ok);
        }
    }

    #[test]
    fn test_fixed_ring_queue() {
        let mut queue = RingQueue::with_capacity(10);
        for num in 0..20 {
            queue.push(num);
            assert_eq!(queue.len(), cmp::min(num + 1, 10));
        }
        assert_eq!(None, queue.swap_remove_front(|i| *i == 20));
        for i in 0..6 {
            assert_eq!(Some(12 + i), queue.swap_remove_front(|e| *e == 12 + i));
            assert_eq!(queue.len(), 9 - i);
        }

        let left: Vec<_> = queue.iter().cloned().collect();
        assert_eq!(vec![10, 11, 18, 19], left);
        for _ in 0..4 {
            queue.swap_remove_front(|_| true).unwrap();
        }
        assert_eq!(None, queue.swap_remove_front(|_| true));
    }

    #[test]
    fn test_defer() {
        let should_panic = Rc::new(AtomicBool::new(true));
        let sp = Rc::clone(&should_panic);
        defer!(assert!(!sp.load(Ordering::SeqCst)));
        should_panic.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_rwlock_deadlock() {
        // If the test runs over 60s, then there is a deadlock.
        let mu = RwLock::new(Some(1));
        {
            let _clone = foo(&mu.rl());
            let mut data = mu.wl();
            assert!(data.is_some());
            *data = None;
        }

        {
            match foo(&mu.rl()) {
                Some(_) | None => {
                    let res = mu.try_write();
                    assert!(res.is_err());
                }
            }
        }

        #[allow(clone_on_copy)]
        fn foo(a: &Option<usize>) -> Option<usize> {
            a.clone()
        }
    }

    #[test]
    fn test_limit_size() {
        let mut e = Entry::new();
        e.set_data(b"0123456789".to_vec());
        let size = u64::from(e.compute_size());

        let tbls = vec![
            (vec![], NO_LIMIT, 0),
            (vec![], size, 0),
            (vec![e.clone(); 10], 0, 1),
            (vec![e.clone(); 10], NO_LIMIT, 10),
            (vec![e.clone(); 10], size, 1),
            (vec![e.clone(); 10], size + 1, 1),
            (vec![e.clone(); 10], 2 * size, 2),
            (vec![e.clone(); 10], 10 * size - 1, 9),
            (vec![e.clone(); 10], 10 * size, 10),
            (vec![e.clone(); 10], 10 * size + 1, 10),
        ];

        for (mut entries, max, len) in tbls {
            limit_size(&mut entries, max);
            assert_eq!(entries.len(), len);
        }
    }

    #[test]
    fn test_slices_vec_deque() {
        for first in 0..10 {
            let mut v = VecDeque::with_capacity(10);
            for i in 0..first {
                v.push_back(i);
            }
            for i in first..10 {
                v.push_back(i - first);
            }
            v.drain(..first);
            for i in 0..first {
                v.push_back(10 + i - first);
            }
            for len in 0..10 {
                for low in 0..len + 1 {
                    for high in low..len + 1 {
                        let (p1, p2) = super::slices_in_range(&v, low, high);
                        let mut res = vec![];
                        res.extend_from_slice(p1);
                        res.extend_from_slice(p2);
                        let exp: Vec<_> = (low..high).collect();
                        assert_eq!(
                            res,
                            exp,
                            "[{}, {}) in {:?} with first: {}",
                            low,
                            high,
                            v,
                            first
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_cfs_diff() {
        let a = vec!["1", "2", "3"];
        let a_diff_a = cfs_diff(&a, &a);
        assert!(a_diff_a.is_empty());
        let b = vec!["4"];
        assert_eq!(a, cfs_diff(&a, &b));
        let c = vec!["4", "5", "3", "6"];
        assert_eq!(vec!["1", "2"], cfs_diff(&a, &c));
        assert_eq!(vec!["4", "5", "6"], cfs_diff(&c, &a));
        let d = vec!["1", "2", "3", "4"];
        let a_diff_d = cfs_diff(&a, &d);
        assert!(a_diff_d.is_empty());
        assert_eq!(vec!["4"], cfs_diff(&d, &a));
    }

    #[test]
    fn test_must_consume_vec() {
        let mut v = MustConsumeVec::new("test");
        v.push(2);
        v.push(3);
        assert_eq!(v.len(), 2);
        v.drain(..);
    }

    #[test]
    fn test_resource_leak() {
        let res = recover_safe!(|| {
            let mut v = MustConsumeVec::new("test");
            v.push(2);
        });
        res.unwrap_err();
    }
}
