use crate::*;

/// blob is similar to Vec<u8>
pub struct Blob<'a, A: AsRedis<'a>, K: std::borrow::Borrow<[u8]>>
{
    client: A,
    key: K,
    phantom: std::marker::PhantomData<&'a ()>
}

impl<'a, A: AsRedis<'a>, K: std::borrow::Borrow<[u8]>> Blob<'a, A, K> {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key, phantom: std::marker::PhantomData }
    }

    fn initiate(&'a mut self, cmd: &[u8]) -> Session<'a, A> {
        Session::new(&self.client).arg(cmd).arg(self.key.borrow())
    }

    pub fn set(&'a mut self, v: &[u8]) {
        self.initiate(b"set").arg(v).fetch().ignore()
    }
}
