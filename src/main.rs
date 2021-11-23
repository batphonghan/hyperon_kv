use std::env;

use hyperon_kv::HyperonStore;

const USAGE: &str = "
Usage: 
    hyperon FILE get KEY
    hyperon FILE update KEY VALUE
    hyperon FILE delete KEY
    hyperon FILE insert KEY VALUE
";
fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let fname = args.get(1).expect(&USAGE);
    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE).as_ref();
    let maybe_value = args.get(4);

    println!(
        "Action: {}, key: {:?}, value: {:?}",
        action, key, maybe_value
    );
    let mut store = HyperonStore::open(&fname)?;
    match action {
        "get" => match store.get(key)? {
            None => eprintln!("Not found key: {:?}", key),
            Some(v) => println!("Founded: {:?}", v),
        },
        "insert" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.insert(key, value)?;
        }
        "update" => {
            let value = maybe_value.expect(&USAGE).as_ref();
            store.update(key, value)?;
        }
        "delete" => {
            store.delete(key)?;
        }
        _ => eprintln!("{}", &USAGE),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // we will write our tests here
    use super::*;

    #[test]
    fn test_load_index() {
        let mut store = HyperonStore::open("hyperon.db").unwrap();
        store.insert(b"1", b"1").unwrap();
        store.insert(b"2", b"2").unwrap();
        store.insert(&[1u8], b"3").unwrap();
        store.insert(&[2u8], b"3").unwrap();

        let v = store.get(b"1").unwrap().unwrap();
        assert_eq!(b"1".to_vec(), v);

        let v = store.get(b"2").unwrap().unwrap();
        assert_eq!(b"2".to_vec(), v);

        let v = store.get(&[1u8]).unwrap().unwrap();
        assert_eq!(b"3".to_vec(), v);

        let v = store.get(&[2u8]).unwrap().unwrap();
        assert_eq!(b"3".to_vec(), v);

        store.update(b"1", b"1.11").unwrap();
        let v = store.get(b"1").unwrap().unwrap();
        assert_eq!(b"1.11".to_vec(), v);

        store.delete(b"1").unwrap();
        let v = store.get(b"1").unwrap();
        assert_eq!(None, v);
    }
}
