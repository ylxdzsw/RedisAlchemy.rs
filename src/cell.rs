use crate::*;
use std::borrow::Borrow;

/// Cell is a container that can hold only one value.
pub struct Cell<A, K, T>
{
    client: A,
    key: K,
    serializer: fn(x: &T) -> Box<[u8]>,
    deserializer: fn(x: &[u8]) -> T
}

impl<A: AsRedis + Clone, K: Borrow<[u8]>, T> Cell<A, K, T> {
    pub fn new(client: A, key: K, serializer: fn(x: &T) -> Box<[u8]>, deserializer: fn(x: &[u8]) -> T) -> Self {
        Self { client, key, serializer, deserializer }
    }

    fn initiate(&self, cmd: &[u8]) -> Session<A::P> {
        self.client.clone().arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
    }

    pub fn set(&self, v: impl Borrow<T>) -> Result<(), RedisError> {
        self.initiate(b"set").arg(&(self.serializer)(v.borrow())).fetch().map(|x| x.ignore())
    }

    pub fn get(&self) -> Result<T, RedisError> {
        self.initiate(b"get").fetch().map(|x| (self.deserializer)(&x.bytes()))
    }

    pub fn clear(&self) -> Result<(), RedisError> {
        self.initiate(b"del").fetch().map(|x| x.ignore())
    }
}
