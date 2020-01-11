use crate::*;

/// blob is conceptually similar to Vec<u8>
pub struct Blob<A, K>
{
    client: A,
    key: K
}

impl<A, K: std::borrow::Borrow<[u8]>> Blob<A, K> where for<'a> &'a A: AsRedis {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key }
    }

    fn initiate(&self, cmd: &[u8]) -> Session<<&A as AsRedis>::P> {
        self.client.arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
    }

    pub fn set(&self, v: &[u8]) -> Result<(), RedisError> {
        self.initiate(b"set").arg(v).fetch().map(|x| x.ignore())
    }

    pub fn get(&self) -> Result<Box<[u8]>, RedisError> {
        self.initiate(b"get").fetch().map(|x| x.bytes())
    }

    pub fn clear(&self) -> Result<(), RedisError> {
        self.initiate(b"del").fetch().map(|x| x.ignore())
    }
}
