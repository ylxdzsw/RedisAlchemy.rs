use crate::*;

/// blob is conceptually similar to Vec<u8>
pub struct Blob<A, K>
{
    client: A,
    key: K
}

impl<'a, A: AsRedis<'a>, K: std::borrow::Borrow<[u8]>> Blob<A, K> {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key }
    }

    fn initiate(&'a self, cmd: &[u8]) -> Session<A::P> {
        self.client.arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
    }

    pub fn set(&'a mut self, v: &[u8]) {
        self.initiate(b"set").arg(v).fetch().ignore()
    }

    pub fn get(&'a self) -> Box<[u8]> {
        self.initiate(b"get").fetch().unwrap().bytes()
    }
}
