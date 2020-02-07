use redis_alchemy::*;

#[test]
fn map() {
    let client = TcpClient::new("127.0.0.1:6379");
    let map: Map<_, _, String, i64> = Map::new(client, &b"map"[..]);
    map.clear().unwrap();

    map.insert(&"a".to_string(), &2).unwrap();
    map.insert(&"b".to_string(), &3).unwrap();

    assert_eq!(map.get(&"a".to_string()).unwrap(), Some(2));
    assert_eq!(map.len().unwrap(), 2);
    assert!(map.contains_key(&"b".to_string()).unwrap());
    map.remove(&"b".to_string()).unwrap();
    assert!(!map.contains_key(&"b".to_string()).unwrap());
    assert!(!map.is_empty().unwrap());
    map.remove(&"a".to_string()).unwrap();
    assert!(map.is_empty().unwrap());
}

#[test]
fn map_iter() {
    let client = TcpClient::new("127.0.0.1:6379");
    let map: Map<_, _, String, i64> = Map::new(client, &b"map_iter"[..]);
    map.clear().unwrap();

    for i in 0..1000 {
        map.insert(&i.to_string(), &i).unwrap();
    }

    let mut pairs: Vec<_> = map.iter().collect();
    pairs.sort_by_key(|(_, v)| *v);
    assert_eq!(pairs[233].1, 233);
    assert_eq!(pairs[666].0, "666".to_string());
}
