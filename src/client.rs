use std::os::unix::net::UnixStream;
use std::io::prelude::*;
use oh_my_rust::*;

pub struct Client {
    count: u32,
    buf: Vec<u8>,
    conn: UnixStream
}

impl Client {
    pub fn new(path: impl AsRef<std::path::Path>) -> Self {
        let conn = UnixStream::connect(path).expect("cannot connect to redis");
        Client { count: 0, buf: vec![], conn }
    }

    pub fn arg(&mut self, x: &[u8]) {
        self.count += 1;
        write!(self.buf, "${}\r\n", x.len()).expect("bug");
        self.buf.extend_from_slice(x);
        self.buf.extend_from_slice(b"\r\n")
    }

    // one should drop the connection and log if this errored
    pub fn fetch(&mut self) -> Result<Response, String> {
        // send
        write!(self.conn, "*{}\r\n", self.count).map_err(|_| "failed writing to socket")?;
        self.conn.write_all(&self.buf).map_err(|_| "failed writing to socket")?;
        self.count = 0;
        self.buf.clear();

        // recv
        let mut reader = std::io::BufReader::new(&mut self.conn);
        let res = parse_resp(&mut reader);
        if !reader.buffer().is_empty() {
            return Err("buffer not empty after read.".to_string())
        }
        
        res
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
            let len: u64 = header.parse().msg("protocol error")?;
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