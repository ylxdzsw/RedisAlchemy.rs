use crate::*;

/// blob is similar to Vec<u8>
pub struct Blob<'a, T, S, A, K>
    where for <'b> &'b T: Read + Write, S: DerefMut<Target=T> + 'a, A: AsRedis<'a, T, S>, K: std::borrow::Borrow<[u8]>
{
    client: A,
    key: K,
    phantom: std::marker::PhantomData<&'a (T, S)>
}

impl<'a, T, S, A, K> Blob<'a, T, S, A, K>
    where for <'b> &'b T: Read + Write, S: DerefMut<Target=T>, A: AsRedis<'a, T, S>, K: std::borrow::Borrow<[u8]>
{
    pub fn new(client: A, key: K) -> Self {
        Self { client, key, phantom: std::marker::PhantomData }
    }

    fn initiate(&mut self, cmd: &[u8]) -> Session<T, S> {
        self.client.as_redis().arg(cmd).arg(self.key.borrow())
    }

    pub fn set(&mut self, v: &[u8]) {
        self.initiate(b"set").arg(v).fetch().ignore()
    }
}
