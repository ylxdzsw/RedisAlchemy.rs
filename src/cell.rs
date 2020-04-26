use crate::*;
use std::borrow::Borrow;

/// Cell is a container that can hold only one value.
pub struct Cell<A, C, K, T>
{
    client: C,
    key: K,
    serializer: fn(x: &T) -> Box<[u8]>,
    deserializer: fn(x: &[u8]) -> T,
    phantom: std::marker::PhantomData<A>
}

impl<A, C: Deref<Target=A>, K: Borrow<[u8]>, T> Cell<A, C, K, T> where for<'a> &'a A: AsRedis {
    pub fn new(client: C, key: K, serializer: fn(x: &T) -> Box<[u8]>, deserializer: fn(x: &[u8]) -> T) -> Self {
        Self { client, key, serializer, deserializer, phantom: std::marker::PhantomData }
    }

    fn initiate(&self, cmd: &[u8]) -> Session<<&A as AsRedis>::P> {
        self.client.arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
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
