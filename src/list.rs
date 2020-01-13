use crate::*;
use std::borrow::Borrow;
use std::collections::VecDeque;
use std::ops::{RangeBounds, Bound};

/// List is conceptually similar to Vec<T>
pub struct List<A, K, T=Box<[u8]>>
{
    client: A,
    key: K,
    phantom: std::marker::PhantomData<T>
}

fn serialization_error<T>(_: T) -> RedisError {
    RedisError::OtherError("Serialization failed".to_string())
}

fn deserialization_error<T>(_: T) -> RedisError {
    RedisError::OtherError("Deserialization failed".to_string())
}

impl<A, K: Borrow<[u8]>, T: serde::Serialize + serde::de::DeserializeOwned> List<A, K, T> where for<'a> &'a A: AsRedis {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key, phantom: std::marker::PhantomData }
    }

    fn initiate(&self, cmd: &[u8]) -> Session<<&A as AsRedis>::P> {
        self.client.arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
    }

    pub fn clear(&self) -> Result<(), RedisError> {
        self.initiate(b"del").fetch().map(|x| x.ignore())
    }

    pub fn push(&self, x: &T) -> Result<(), RedisError> {
        let buf = serde_json::to_vec(x).map_err(serialization_error)?;
        self.initiate(b"rpush").arg(&buf).fetch().map(|x| x.ignore())
    }

    pub fn get(&self, i: i64) -> Result<Option<T>, RedisError> {
        match self.initiate(b"lindex").arg(i.to_string().as_bytes()).fetch()? {
            Response::Bytes(x) => serde_json::from_slice(&x).map_err(deserialization_error),
            Response::Nothing => Ok(None),
            _ => unreachable!()
        }
    }

    pub fn iter(&self) -> impl Iterator<Item=T> + '_ {
        self.into_iter()
    }

    // Note: the end bound is *included* in redis
    pub fn range(&self, range: impl RangeBounds<i64>) -> Result<Box<[T]>, RedisError> {
        let start = match range.start_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => if *x == -1 {
                return Ok(vec![].into())
            } else {
                x + 1
            },
            Bound::Unbounded => 0
        } as i64;

        let end = match range.end_bound() {
            Bound::Included(x) => *x,
            Bound::Excluded(x) => if *x == 0 {
                return Ok(vec![].into())
            } else {
                x - 1
            },
            Bound::Unbounded => -1
        };

        self.initiate(b"lrange")
            .arg(start.to_string().as_bytes())
            .arg(end.to_string().as_bytes())
            .fetch()?.list().into_iter()
            .map(|x| serde_json::from_slice(&x.bytes()).map_err(deserialization_error))
            .collect()
    }
}

const BATCH_SIZE: usize = 12;

pub struct ListIter<'l, A, K, T> {
    buf: VecDeque<T>,
    index: usize,
    list: &'l List<A, K, T>
}

impl<'l, A, K: Borrow<[u8]>, T: serde::Serialize + serde::de::DeserializeOwned> Iterator for ListIter<'l, A, K, T> where for<'a> &'a A: AsRedis {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() { // try to get a batch
            let batch = self.list.initiate(b"lrange")
                .arg(self.index.to_string().as_bytes())
                .arg((self.index + BATCH_SIZE).to_string().as_bytes())
                .fetch().expect("Error during iteration").list();

            self.index += batch.len();
            for x in batch.into_iter() {
                let x = serde_json::from_slice(&x.bytes()).map_err(deserialization_error).expect("Error during iteration");
                self.buf.push_back(x)
            }
        }

        self.buf.pop_front()
    }
}

impl<'l, A, K: Borrow<[u8]>, T: serde::Serialize + serde::de::DeserializeOwned> IntoIterator for &'l List<A, K, T> where for<'a> &'a A: AsRedis {
    type Item = T;
    type IntoIter = ListIter<'l, A, K, T>;

    fn into_iter(self) -> Self::IntoIter {
        ListIter { buf: VecDeque::with_capacity(BATCH_SIZE), index: 0, list: self }
    }
}
