use crate::*;
use std::borrow::Borrow;
use std::collections::VecDeque;
use std::ops::{RangeBounds, Bound};

/// Maps associate fields with values
pub struct Map<A, C, K, F, V>
{
    client: C,
    key: K,
    field_serializer: fn(x: &F) -> Box<[u8]>,
    field_deserializer: fn(x: &[u8]) -> F,
    value_serializer: fn(x: &V) -> Box<[u8]>,
    value_deserializer: fn(x: &[u8]) -> V,
    phantom: std::marker::PhantomData<A>
}

impl<A, C: Deref<Target=A>, K: Borrow<[u8]>, F, V> Map<A, C, K, F, V> where for<'a> &'a A: AsRedis {
    pub fn new(
        client: C, key: K,
        field_serializer: fn(x: &F) -> Box<[u8]>,
        field_deserializer: fn(x: &[u8]) -> F,
        value_serializer: fn(x: &V) -> Box<[u8]>,
        value_deserializer: fn(x: &[u8]) -> V
    ) -> Self {
        Self { client, key, field_serializer, field_deserializer, value_serializer, value_deserializer, phantom: std::marker::PhantomData }
    }

    fn initiate(&self, cmd: &[u8]) -> Session<<&A as AsRedis>::P> {
        self.client.arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
    }

    pub fn clear(&self) -> Result<(), RedisError> {
        self.initiate(b"del").fetch().map(|x| x.ignore())
    }

    pub fn get(&self, field: impl Borrow<F>) -> Result<Option<V>, RedisError> {
        match self.initiate(b"hget").arg(&(self.field_serializer)(field.borrow())).fetch()? {
            Response::Bytes(x) => Ok(Some((self.value_deserializer)(&x))),
            Response::Nothing => Ok(None),
            _ => unreachable!()
        }
    }

    pub fn insert(&self, field: impl Borrow<F>, value: impl Borrow<V>) -> Result<(), RedisError> {
        self.initiate(b"hset")
            .arg(&(self.field_serializer)(field.borrow()))
            .arg(&(self.value_serializer)(value.borrow()))
            .fetch().map(|x| x.ignore())
    }

    pub fn remove(&self, field: impl Borrow<F>) -> Result<(), RedisError> {
        self.initiate(b"hdel").arg(&(self.field_serializer)(field.borrow())).fetch().map(|x| x.ignore())
    }

    pub fn contains_key(&self, field: impl Borrow<F>) -> Result<bool, RedisError> {
        self.initiate(b"hexists").arg(&(self.field_serializer)(field.borrow())).fetch().map(|x| x.integer() == 1)
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

pub struct MapIter<'m, A, C, K, F, V> {
    buf: VecDeque<(F, V)>,
    cursor: Box<[u8]>,
    map: &'m Map<A, C, K, F, V>,
    done: bool
}

impl<'m, A, C: Deref<Target=A>, K: Borrow<[u8]>, F, V> Iterator for MapIter<'m, A, C, K, F, V> where for<'a> &'a A: AsRedis {
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
                    let value = (self.map.value_deserializer)(&x.bytes());
                    self.buf.push_back((field, value));
                    current_field = None
                } else {
                    let field = (self.map.field_deserializer)(&x.bytes());
                    current_field = Some(field)
                }
            }
        }

        self.buf.pop_front()
    }
}

impl<'m, A, C: Deref<Target=A>, K: Borrow<[u8]>, F, V> IntoIterator for &'m Map<A, C, K, F, V> where for<'a> &'a A: AsRedis {
    type Item = (F, V);
    type IntoIter = MapIter<'m, A, C, K, F, V>;

    fn into_iter(self) -> Self::IntoIter {
        MapIter { buf: VecDeque::with_capacity(BATCH_HINT), cursor: b"0"[..].into(), map: self, done: false }
    }
}
