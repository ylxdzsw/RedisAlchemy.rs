use crate::*;
use std::borrow::Borrow;
use std::collections::VecDeque;
use std::ops::{RangeBounds, Bound};

/// List is conceptually similar to Vec<T>
pub struct List<A, C, K, T>
{
    client: C,
    key: K,
    serializer: fn(x: &T) -> Box<[u8]>,
    deserializer: fn(x: &[u8]) -> T,
    phantom: std::marker::PhantomData<A>
}

impl<A, C: Deref<Target=A>, K: Borrow<[u8]>, T> List<A, C, K, T> where for<'a> &'a A: AsRedis {
    pub fn new(client: C, key: K, serializer: fn(x: &T) -> Box<[u8]>, deserializer: fn(x: &[u8]) -> T) -> Self {
        Self { client, key, serializer, deserializer, phantom: std::marker::PhantomData }
    }

    fn initiate(&self, cmd: &[u8]) -> Session<<&A as AsRedis>::P> {
        self.client.arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
    }

    pub fn clear(&self) -> Result<(), RedisError> {
        self.initiate(b"del").fetch().map(|x| x.ignore())
    }

    pub fn push(&self, x: impl Borrow<T>) -> Result<(), RedisError> {
        self.initiate(b"rpush").arg(&(self.serializer)(x.borrow())).fetch().map(|x| x.ignore())
    }

    pub fn extend(&self, x: &[impl Borrow<T>]) -> Result<(), RedisError> { // TODO: push in batch if the number is too big
        if x.is_empty() {
            return Ok(())
        }

        let mut sess = self.initiate(b"rpush");
        for v in x {
            sess.arg(&(self.serializer)(v.borrow()));
        }
        sess.fetch().map(|x| x.ignore())
    }

    pub fn push_front(&self, x: impl Borrow<T>) -> Result<(), RedisError> {
        self.initiate(b"lpush").arg(&(self.serializer)(x.borrow())).fetch().map(|x| x.ignore())
    }

    pub fn pop(&self) -> Result<Option<T>, RedisError> {
        match self.initiate(b"rpop").fetch()? {
            Response::Bytes(x) => Ok(Some((self.deserializer)(&x))),
            Response::Nothing => Ok(None),
            _ => unreachable!()
        }
    }

    pub fn pop_front(&self) -> Result<Option<T>, RedisError> {
        match self.initiate(b"lpop").fetch()? {
            Response::Bytes(x) => Ok(Some((self.deserializer)(&x))),
            Response::Nothing => Ok(None),
            _ => unreachable!()
        }
    }

    /// blocking pop_front. return None when timeout reached. timeout is the number of seconds to wait. 0 means waiting indefinitely.
    pub fn recv(&self, timeout: i64) -> Result<Option<T>, RedisError> {
        match self.initiate(b"blpop").arg(timeout.to_string().as_bytes()).fetch()? {
            Response::List(x) => Ok(Some((self.deserializer)(x[1].as_bytes()))), // x[0] is the key since `blpop` supports polling multiple keys
            Response::Nothing => Ok(None),
            _ => unreachable!()
        }
    }

    pub fn get(&self, i: i64) -> Result<Option<T>, RedisError> {
        match self.initiate(b"lindex").arg(i.to_string().as_bytes()).fetch()? {
            Response::Bytes(x) => Ok(Some((self.deserializer)(&x))),
            Response::Nothing => Ok(None),
            _ => unreachable!()
        }
    }

    /// Sets the list element at i to v. An error is returned for out of range indexes.
    pub fn set(&self, i: i64, v: impl Borrow<T>) -> Result<(), RedisError> {
        self.initiate(b"lset").arg(i.to_string().as_bytes()).arg(&(self.serializer)(v.borrow())).fetch().map(|x| x.ignore())
    }

    pub fn iter(&self) -> impl Iterator<Item=T> + '_ {
        self.into_iter()
    }

    pub fn to_vec(&self) -> Result<Vec<T>, RedisError> {
        self.range(..).map(|x| x.into_vec())
    }

    pub fn len(&self) -> Result<usize, RedisError> {
        self.initiate(b"llen").fetch().map(|x| x.integer() as _)
    }

    pub fn is_empty(&self) -> Result<bool, RedisError> {
        Ok(self.len()? == 0)
    }

    pub fn sort_numeric(&self) -> Result<(), RedisError> {
        unimplemented!()
    }

    pub fn sort_alphabetic(&self) -> Result<(), RedisError> {
        unimplemented!()
    }

    pub fn trim(&self, _start: i64, _end: i64) -> Result<(), RedisError> {
        unimplemented!()
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

        Ok(self.initiate(b"lrange")
            .arg(start.to_string().as_bytes())
            .arg(end.to_string().as_bytes())
            .fetch()?.list().into_iter()
            .map(|x| (self.deserializer)(&x.bytes()))
            .collect())
    }
}

const BATCH_SIZE: usize = 12;

pub struct ListIter<'l, A, C, K, T> {
    buf: VecDeque<T>,
    index: usize,
    list: &'l List<A, C, K, T>
}

impl<'l, A, C: Deref<Target=A>, K: Borrow<[u8]>, T> Iterator for ListIter<'l, A, C, K, T> where for<'a> &'a A: AsRedis {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.is_empty() { // try to get a batch
            let batch = self.list.initiate(b"lrange")
                .arg(self.index.to_string().as_bytes())
                .arg((self.index + BATCH_SIZE).to_string().as_bytes())
                .fetch().expect("Error during iteration").list();

            self.index += batch.len();
            for x in batch.into_iter() {
                let x = (self.list.deserializer)(&x.bytes());
                self.buf.push_back(x)
            }
        }

        self.buf.pop_front()
    }
}

impl<'l, A, C: Deref<Target=A>, K: Borrow<[u8]>, T> IntoIterator for &'l List<A, C, K, T> where for<'a> &'a A: AsRedis {
    type Item = T;
    type IntoIter = ListIter<'l, A, C, K, T>;

    fn into_iter(self) -> Self::IntoIter {
        ListIter { buf: VecDeque::with_capacity(BATCH_SIZE), index: 0, list: self }
    }
}
