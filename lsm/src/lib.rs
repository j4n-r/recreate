use bytes::Bytes;
use std::{
    collections::BTreeMap,
    fs::{self},
    io::{self, Read, Write},
    path::Path,
    sync::{Arc, RwLock},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("io error")]
    Io(#[from] io::Error),
    #[error("serde error")]
    Serialize(#[from] serde_json::Error),
    #[error("database lock was poisoned")]
    LockPoisoned,
}

pub struct Db {
    map: Arc<RwLock<BTreeMap<Bytes, Bytes>>>,
    wal: Wal,
}

impl Db {
    pub fn create(log_path: impl AsRef<Path>) -> Result<Self, DbError> {
        let mut wal = Wal::create(log_path)?;
        let mut content = String::new();
        wal.file.read_to_string(&mut content)?;

        let mut btree = BTreeMap::new();
        content.split('\n').for_each(|line| {
            if let Some((key, value)) = line.split_once(',') {
                btree.insert(
                    Bytes::copy_from_slice(key.as_bytes()),
                    Bytes::copy_from_slice(value.as_bytes()),
                );
            }
        });
        let map = Arc::new(RwLock::new(btree));

        Ok(Self {
            map,
            wal,
        })
    }
    pub fn put(&mut self, key: &str, value: &str) -> Result<(), DbError> {
        self.wal.write(key, value)?;
        self.map.write().map_err(|_| DbError::LockPoisoned)?.insert(
            Bytes::copy_from_slice(key.as_bytes()),
            Bytes::copy_from_slice(value.as_bytes()),
        );
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Bytes>, DbError> {
        let map = self.map.read().map_err(|_| DbError::LockPoisoned)?;
        Ok(map
            .get(key.as_bytes())
            .cloned()
            .filter(|value| !value.is_empty()))
    }

    pub fn delete(&mut self, key: &str) -> Result<(), DbError> {
        self.put(key, "")
    }
}

pub struct Wal {
    file: fs::File,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self { file })
    }
    pub fn write(&mut self, key: &str, value: &str) -> Result<(), DbError> {
        writeln!(self.file, "{key},{value}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_value() -> Result<(), DbError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let mut db = Db::create(&path)?;

        db.put("20", "hi")?;

        let got = db.get("20")?;
        assert_eq!(got, Some(Bytes::from("hi")));
        Ok(())
    }

    #[test]
    fn put_then_delete_then_get_returns_nothing() -> Result<(), DbError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let mut db = Db::create(&path)?;

        db.put("20", "hi")?;
        db.delete("20")?;

        let got = db.get("20")?;
        assert_eq!(got, None);
        Ok(())
    }
    #[test]
    fn delete_does_note_crash() -> Result<(), DbError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let mut db = Db::create(&path)?;

        db.delete("20")?;
        Ok(())
    }
    #[test]
    fn get_not_found() -> Result<(), DbError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let db = Db::create(&path)?;

        assert_eq!(db.get("20")?, None);
        Ok(())
    }
    #[test]
    fn update_value() -> Result<(), DbError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let mut db = Db::create(&path)?;

        db.put("20", "hi")?;
        assert_eq!(db.get("20")?, Some(Bytes::from("hi")));
        db.put("20", "bye")?;
        assert_eq!(db.get("20")?, Some(Bytes::from("bye")));
        Ok(())
    }
}
