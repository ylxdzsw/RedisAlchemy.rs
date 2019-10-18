#![allow(irrefutable_let_patterns)]
#![allow(dead_code, unused_imports)]
#![allow(non_camel_case_types)]
#![deny(bare_trait_objects)]
#![warn(clippy::all)]
#![allow(clippy::write_with_newline)]

// AsRedis: primary API. Anything that can initiate a valid redis session. Typically RefCell<UnixStream> and the clients. A AsRedis may fail if it is already in use, or pending if it needs to wait before makeing a new connection.
// Command: a transilent command builder buffer. Hold a unique reference to the connection (be it shared or not).
// Collection: a collection represents the data behind a redis key.
// Response: an enum of possible non-error return types from Redis.

mod blob;

use std::os::unix::net::UnixStream;
use std::net::TcpStream;
use std::io::prelude::*;
use oh_my_rust::*;

pub trait AsRedis<'a, T, S: std::ops::DerefMut<Target=T>>
    where for <'b> &'b T: Read + Write
{
    fn as_redis(&'a self) -> Session<T, S>;
}

impl<'a> AsRedis<'a, UnixStream, std::cell::RefMut<'a, UnixStream>> for std::cell::RefCell<UnixStream> {
    fn as_redis(&'a self) -> Session<UnixStream, std::cell::RefMut<'a, UnixStream>> {
        Session::new(self.borrow_mut())
    }
}

impl<'a> AsRedis<'a, TcpStream, std::cell::RefMut<'a, TcpStream>> for std::cell::RefCell<TcpStream> {
    fn as_redis(&'a self) -> Session<TcpStream, std::cell::RefMut<'a, TcpStream>> {
        Session::new(self.borrow_mut())
    }
}

// for convenience before we design and impl the various auto-managing clients and pools
impl<'a, T: AsRef<std::path::Path> + ?Sized> AsRedis<'a, UnixStream, Box<UnixStream>> for T {
    fn as_redis(&'a self) -> Session<UnixStream, Box<UnixStream>> {
        let conn = UnixStream::connect(self).expect("cannot connect to redis");
        Session::new(Box::new(conn))
    }
}

impl<'a, T: std::net::ToSocketAddrs + ?Sized> AsRedis<'a, TcpStream, Box<TcpStream>> for T {
    fn as_redis(&'a self) -> Session<TcpStream, Box<TcpStream>> {
        let conn = TcpStream::connect(self).expect("cannot connect to redis");
        Session::new(Box::new(conn))
    }
}

pub struct Session<T, S: std::ops::DerefMut<Target=T>>
    where for <'a> &'a T: Read + Write, T: ?Sized
{
    count: u32,
    buf: Vec<u8>,
    conn: Option<S>
}

impl<T: Read + Write, S: std::ops::DerefMut<Target=T>> Session<T, S>
    where for <'a> &'a T: Read + Write
{
    pub fn new(conn: S) -> Self {
        Session { count: 0, buf: vec![], conn: Some(conn) }
    }

    pub fn arg(&mut self, x: &[u8]) -> &mut Self {
        self.count += 1;
        write!(self.buf, "${}\r\n", x.len()).expect("bug");
        self.buf.extend_from_slice(x);
        self.buf.extend_from_slice(b"\r\n");
        self // for chaining
    }

    // one should drop the connection if this errored
    pub fn fetch(&mut self) -> Result<Response, String> {
        let mut conn = self.conn.take().expect("bug");
        // send
        write!(conn, "*{}\r\n", self.count).map_err(|_| "failed writing to socket")?;
        conn.write_all(&self.buf).map_err(|_| "failed writing to socket")?;
        self.count = 0;
        self.buf.clear();

        // recv
        let mut reader = std::io::BufReader::new(&*conn);
        let res = parse_resp(&mut reader);
        if !reader.buffer().is_empty() {
            return Err("buffer not empty after read.".to_string())
        }
        self.conn = Some(conn);

        res
    }
    
    pub fn run(&mut self) -> &mut Self {
        self.fetch().expect("redis failed");
        self // for chaining
    }
}

fn parse_resp(r: &mut impl BufRead) -> Result<Response, String> {
    let mut header = String::new();
    r.read_line(&mut header).unwrap();

    let magic = header.as_bytes()[0];
    let header = header[1..].trim_end();

    match magic {
        b'+' => Ok(Response::Text(header.to_string())),
        b'-' => Err(format!("redis error: {}", header)),
        b':' => Ok(Response::Integer(header.parse().msg("protocol error")?)),
        b'$' => if header.as_bytes()[0] == b'-' {
            Ok(Response::Nothing)
        } else {
            let len: u64 = header.parse().msg("protocol error")?;
            let mut buf = r.read_exact_alloc((len + 2) as usize).msg("protocol error")?;
            buf.truncate(len as usize); // remove the new line terminator
            Ok(Response::Bytes(buf.into_boxed_slice()))
        },
        b'*' => if header.as_bytes()[0] == b'-' {
            Ok(Response::Nothing)
        } else {
            let len: u32 = header.parse().msg("protocol error")?;
            Ok(Response::List((0..len).map(|_| {
                parse_resp(r)
            }).collect::<Result<Vec<_>, _>>()?))
        },
        _ => Err("protocol error".to_string())
    }
}

pub enum Response {
    Integer(i64), Text(String), Bytes(Box<[u8]>), List(Vec<Response>), Nothing
}

impl Response {
    pub fn into_integer(self) -> i64 {
        if let Response::Integer(x) = self {
            x
        } else {
            panic!("not an integer")
        }
    }

    pub fn try_into_integer(self) -> Option<i64> {
        if let Response::Integer(x) = self {
            Some(x)
        } else {
            None
        }
    }

    pub fn into_text(self) -> String {
        if let Response::Text(x) = self {
            x
        } else {
            panic!("not a text")
        }
    }

    pub fn try_into_text(self) -> Option<String> {
        if let Response::Text(x) = self {
            Some(x)
        } else {
            None
        }
    }

    pub fn into_bytes(self) -> Box<[u8]> {
        if let Response::Bytes(x) = self {
            x
        } else {
            panic!("not bytes")
        }
    }

    pub fn try_into_bytes(self) -> Option<Box<[u8]>> {
        if let Response::Bytes(x) = self {
            Some(x)
        } else {
            None
        }
    }

    pub fn into_list(self) -> Vec<Response> {
        if let Response::List(x) = self {
            x
        } else {
            panic!("not bytes")
        }
    }

    pub fn try_into_list(self) -> Option<Vec<Response>> {
        if let Response::List(x) = self {
            Some(x)
        } else {
            None
        }
    }

    #[allow(clippy::unused_unit)]
    pub fn into_nothing(self) -> () {
        if let Response::Nothing = self {
            ()
        } else {
            panic!("not nothing")
        }
    }

    pub fn try_into_nothing(self) -> Option<()> {
        if let Response::Nothing = self {
            Some(())
        } else {
            None
        }
    }
}