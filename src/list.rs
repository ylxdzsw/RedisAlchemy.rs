use crate::*;
use std::borrow::Borrow;

/// List is conceptually similar to Vec<T>
pub struct List<A, K, T=Box<[u8]>>
{
    client: A,
    key: K,
    phantom: std::marker::PhantomData<T>
}

impl<'a, A: AsRedis<'a>, K: Borrow<[u8]>, T: serde::Serialize + serde::Deserialize<'static>> List<A, K, T> {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key, phantom: std::marker::PhantomData }
    }

    fn initiate(&'a self, cmd: &[u8]) -> Session<A::P> {
        self.client.arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
    }

    pub fn clear(&'a self) -> Result<(), RedisError> {
        self.initiate(b"del").fetch().map(|x| x.ignore())
    }

    pub fn push(&'a self, x: &T) -> Result<(), RedisError> {
        let buf = serde_json::to_vec(x).map_err(|_| RedisError::OtherError("Serialization failed".to_string()))?;
        self.initiate(b"rpush").arg(&buf).fetch().map(|x| x.ignore())
    }
}
