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

    pub fn set(&'a self, v: &[u8]) -> Result<(), RedisError> {
        self.initiate(b"set").arg(v).fetch().map(|x| x.ignore())
    }

    pub fn get(&'a self) -> Result<Box<[u8]>, RedisError> {
        self.initiate(b"get").fetch().map(|x| x.bytes())
    }

    pub fn clear(&'a self) -> Result<(), RedisError> {
        self.initiate(b"del").fetch().map(|x| x.ignore())
    }
}
