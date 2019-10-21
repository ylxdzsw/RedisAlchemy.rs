use crate::*;

/// blob is similar to Vec<u8>
pub struct Blob<'a, 'b, A: AsRedis<'a, 'b>, K: std::borrow::Borrow<[u8]>>
{
    client: A,
    key: K,
    phantom: std::marker::PhantomData<(&'a (), &'b ())>
}

impl<'a, 'b, A: AsRedis<'a, 'b>, K: std::borrow::Borrow<[u8]>> Blob<'a, 'b, A, K> {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key, phantom: std::marker::PhantomData }
    }

    fn initiate(&'b mut self, cmd: &[u8]) -> Session<'a, 'b, A> {
        Session::new(&self.client).arg(cmd).arg(self.key.borrow())
    }

    pub fn set(&'b mut self, v: &[u8]) {
        self.initiate(b"set").arg(v).fetch().ignore()
    }
}
