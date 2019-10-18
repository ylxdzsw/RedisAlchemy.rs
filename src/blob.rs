// use crate::*;

// /// blob is similar to Vec<u8>
// pub struct Blob<'a, T, C, K>
//     where C: AsRedis<'a, T>, T: Read + Write, K: std::borrow::Borrow<[u8]>
// {
//     client: C,
//     key: K,
//     phantom: std::marker::PhantomData<&'a T>
// }

// impl<'a, T, C, K> Blob<'a, T, C, K>
//     where C: AsRedis<'a, T>, T: Read + Write, K: std::borrow::Borrow<[u8]>
// {
//     pub fn new(client: C, key: K) -> Self {
//         Self { client, key, phantom: std::marker::PhantomData }
//     }

//     pub fn set(&mut self) {

//     }
// }
