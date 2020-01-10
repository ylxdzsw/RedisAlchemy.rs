use crate::*;
use std::borrow::Borrow;

/// BitVec is conceptually similar to Vec<bool>
pub struct BitVec<A, K>
{
    client: A,
    key: K
}

impl<'a, A: AsRedis<'a>, K: Borrow<[u8]>> BitVec<A, K> {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key }
    }

    fn initiate(&'a self, cmd: &[u8]) -> Session<A::P> {
        self.client.arg(cmd).apply(|x| x.arg(self.key.borrow()).ignore())
    }

    pub fn set_raw(&'a self, v: &[u8]) {
        self.initiate(b"set").arg(v).fetch().ignore()
    }

    pub fn get_raw(&'a self) -> Box<[u8]> {
        self.initiate(b"get").fetch().unwrap().bytes()
    }

    /// get the bit value at `index` (starts from 0). If it is out of range or the key does not exist, return false.
    pub fn get(&'a self, index: usize) -> Result<bool, RedisError> {
        self.initiate(b"getbit").arg(index.to_string().as_bytes()).fetch().map(|x| x.integer() != 0)
    }

    /// set the bit value at `index` (starts from 0).
    pub fn set(&'a self, index: usize, value: bool) -> Result<bool, RedisError> {
        self.initiate(b"setbit")
            .arg(index.to_string().as_bytes())
            .arg(if value { b"1" } else { b"0" })
            .fetch().map(|x| x.integer() != 0)
    }

    pub fn len(&'a self) -> Result<usize, RedisError> {
        let l = self.initiate(b"strlen").fetch()?.integer();
        if l < 0 || l as u64 > (std::usize::MAX / 8) as u64 {
            panic!("length out of range")
        }
        Ok(8 * (l as usize))
    }

    pub fn clear(&'a self) -> Result<(), RedisError> {
        self.initiate(b"del").fetch().map(|x| x.ignore())
    }

    /// count the number of 1 in the BitVec
    pub fn sum(&'a self) -> Result<u64, RedisError> {
        self.initiate(b"bitcount").fetch().map(|x| x.integer() as u64)
    }

    /// return the index of the first 1. None if the BitVec is empty or contains only 0
    pub fn find_first(&'a self) -> Result<Option<usize>, RedisError> {
        self.initiate(b"bitpos").arg(b"1").fetch().map(|x| {
            let x = x.integer();
            if x == -1 {
                None
            } else {
                Some(x as _)
            }
        })
    }
}
