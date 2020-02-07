use crate::*;
use std::borrow::Borrow;
use std::collections::VecDeque;
use std::ops::{RangeBounds, Bound};

/// List is conceptually similar to Vec<T>
pub struct Map<A, K, F=Box<[u8]>, V=Box<[u8]>>
{
    client: A,
    key: K,
    phantom: std::marker::PhantomData<(F, V)>
}

fn serialization_error<F>(_: F) -> RedisError {
    RedisError::OtherError("Serialization failed".to_string())
}

fn deserialization_error<F>(_: F) -> RedisError {
    RedisError::OtherError("Deserialization failed".to_string())
}

impl<A, K: Borrow<[u8]>, F, V> Map<A, K, F, V> where
        for<'a> &'a A: AsRedis,
        F: serde::Serialize + serde::de::DeserializeOwned,
        V: serde::Serialize + serde::de::DeserializeOwned {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key, phantom: std::marker::PhantomData }
    }

    fn initiate(&self, cmd: &[u8]) -> Session<<&A as AsRedis>::P> {
        self.client.arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
    }

    pub fn clear(&self) -> Result<(), RedisError> {
        self.initiate(b"del").fetch().map(|x| x.ignore())
    }

    pub fn get(&self, field: &F) -> Result<Option<V>, RedisError> {
        match self.initiate(b"hget").arg(&serde_json::to_vec(field).map_err(serialization_error)?).fetch()? {
            Response::Bytes(x) => serde_json::from_slice(&x).map_err(deserialization_error),
            Response::Nothing => Ok(None),
            _ => unreachable!()
        }
    }

    pub fn insert(&self, field: &F, value: &V) -> Result<(), RedisError> {
        let field_buf = serde_json::to_vec(field).map_err(serialization_error)?;
        let value_buf = serde_json::to_vec(value).map_err(serialization_error)?;
        self.initiate(b"hset").arg(&field_buf).arg(&value_buf).fetch().map(|x| x.ignore())
    }

    pub fn remove(&self, field: &F) -> Result<(), RedisError> {
        let buf = serde_json::to_vec(field).map_err(serialization_error)?;
        self.initiate(b"hdel").arg(&buf).fetch().map(|x| x.ignore())
    }

    pub fn contains_key(&self, field: &F) -> Result<bool, RedisError> {
        let buf = serde_json::to_vec(field).map_err(serialization_error)?;
        self.initiate(b"hexists").arg(&buf).fetch().map(|x| x.integer() == 1)
    }

    pub fn extend(&self, _pairs: &[(F, V)]) -> Result<(), RedisError> {
        unimplemented!()
    }

    pub fn len(&self) -> Result<usize, RedisError> {
        self.initiate(b"hlen").fetch().map(|x| x.integer() as _)
    }

    pub fn is_empty(&self) -> Result<bool, RedisError> {
        Ok(self.len()? == 0)
    }

    pub fn iter(&self) -> impl Iterator<Item=(F, V)> + '_ {
        self.into_iter()
    }
}

const BATCH_HINT: usize = 12;

pub struct MapIter<'m, A, K, F, V> {
    buf: VecDeque<(F, V)>,
    cursor: Box<[u8]>,
    map: &'m Map<A, K, F, V>,
    done: bool
}

impl<'m, A, K: Borrow<[u8]>, F, V> Iterator for MapIter<'m, A, K, F, V> where
        for<'a> &'a A: AsRedis,
        F: serde::Serialize + serde::de::DeserializeOwned,
        V: serde::Serialize + serde::de::DeserializeOwned {
    type Item = (F, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() { // try to get a batch
            if self.done {
                return None
            }
            let mut res = self.map.initiate(b"hscan")
                .arg(&self.cursor)
                .fetch().expect("Error during iteration").list();

            // Rust sucks in destructing vectors
            let buf = res.pop().unwrap().list();
            let cursor = res.pop().unwrap().bytes();

            if cursor[..] == b"0"[..] {
                self.done = true
            } else {
                self.cursor = cursor;
            }

            let mut current_field = None; // the buf is interleaved with fields and values
            for x in buf.into_iter() {
                if let Some(field) = current_field {
                    let value = serde_json::from_slice(&x.bytes()).map_err(deserialization_error).expect("Error during iteration");
                    self.buf.push_back((field, value));
                    current_field = None
                } else {
                    let field = serde_json::from_slice(&x.bytes()).map_err(deserialization_error).expect("Error during iteration");
                    current_field = Some(field)
                }
            }
        }

        self.buf.pop_front()
    }
}

impl<'m, A, K: Borrow<[u8]>, F, V> IntoIterator for &'m Map<A, K, F, V> where
        for<'a> &'a A: AsRedis,
        F: serde::Serialize + serde::de::DeserializeOwned,
        V: serde::Serialize + serde::de::DeserializeOwned {
    type Item = (F, V);
    type IntoIter = MapIter<'m, A, K, F, V>;

    fn into_iter(self) -> Self::IntoIter {
        MapIter { buf: VecDeque::with_capacity(BATCH_HINT), cursor: b"0"[..].into(), map: self, done: false }
    }
}
