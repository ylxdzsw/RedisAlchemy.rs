use crate::*;

/// blob is similar to Vec<u8>
pub struct Blob<A, K: std::borrow::Borrow<[u8]>>
{
    client: A,
    key: K
}

impl<'a, A: AsRedis<'a>, K: std::borrow::Borrow<[u8]>> Blob<A, K> {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key }
    }

    fn initiate<'b: 'a>(&'b mut self, cmd: &[u8]) -> Session<'a, 'b, A> {
        Session::new(&self.client).apply_owned(|x| x.arg(cmd).arg(self.key.borrow()).ignore())
    }

    pub fn set<'b: 'a>(&'b mut self, v: &[u8]) {
        self.initiate(b"set").arg(v).fetch().ignore()
    }

    pub fn get<'b: 'a>(&'b mut self) -> Box<[u8]> {
        self.initiate(b"get").fetch().unwrap().into_bytes()
    }
}
