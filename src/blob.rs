use crate::*;

/// blob is similar to Vec<u8>
pub struct Blob<'p, A: AsRedis<'p, T>, T: Read + Write, K: std::borrow::Borrow<[u8]>> where for <'b> &'b T: Read + Write
{
    client: A,
    key: K,
    phantom: std::marker::PhantomData<&'p T>
}

impl<'p, A: AsRedis<'p, T>, T: Read + Write, K: std::borrow::Borrow<[u8]>> Blob<'p, A, T, K> where for <'a> &'a T: Read + Write {
    pub fn new(client: A, key: K) -> Self {
        Self { client, key, phantom: std::marker::PhantomData }
    }

    fn initiate(&'p mut self, cmd: &[u8]) -> Session<'p, 'p, A, T> {
        Session::new(&self.client)
            .arg(cmd).arg(self.key.borrow())
    }

    pub fn set(&'p mut self, v: &[u8]) {
        self.initiate(b"set").arg(v).fetch().ignore()
    }
}
