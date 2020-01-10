use redis_alchemy::*;

#[test]
fn bitvec() {
    let client = TcpClient::new("127.0.0.1:6379");
    let bitvec = BitVec::new(client, &b"bitvec"[..]);
    assert!(bitvec.find_first().unwrap().is_none());
    bitvec.set(2, true).unwrap();
    assert_eq!(bitvec.get(1).unwrap(), false);
    assert_eq!(bitvec.get(2).unwrap(), true);
    assert_eq!(bitvec.find_first().unwrap().unwrap(), 2);
    assert_eq!(bitvec.sum().unwrap(), 1);
    bitvec.set(2, false).unwrap();
    assert!(bitvec.find_first().unwrap().is_none());
}
