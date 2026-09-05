use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self},
    io::{self, Read, Write},
    path::Path,
    sync::{Arc, Mutex, RwLock, atomic::AtomicUsize},
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
    memtable: RwLock<Memtable>,
    locked_tables: Vec<Memtable>,
    wal: Wal,
}

static MAX_MEMTABLE_SIZE: u32 = 100;

pub struct Memtable {
    tree: Arc<RwLock<BTreeMap<Bytes, Bytes>>>,
    size: AtomicUsize,
}

impl Db {
    pub fn create(log_path: impl AsRef<Path>) -> Result<Self, DbError> {
        let wal = Wal::create(log_path)?;
        let mut btree: BTreeMap<Bytes, Bytes> = BTreeMap::new();

        let mut buf = String::new();
        {
            let mut file = wal.file.lock().map_err(|_| DbError::LockPoisoned)?;
            file.read_to_string(&mut buf)?;
        }
        for line in buf.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let wal_entry = serde_json::from_str(line)?;
            match wal_entry {
                WalEntry::Put { key, value } => {
                    btree.insert(Bytes::from(key), Bytes::from(value));
                }
                WalEntry::Delete { key } => {
                    btree.insert(key.into(), Bytes::new());
                }
            }
        }

        let tree = Arc::new(RwLock::new(btree));

        Ok(Self {
            wal,
            memtable: RwLock::new(Memtable {
                tree,
                size: AtomicUsize::new(0),
            }),
            locked_tables: Vec::new(),
        })
    }
    pub fn put(&self, key: &str, value: &str) -> Result<(), DbError> {
        self.wal.append(&WalEntry::Put {
            key: key.into(),
            value: value.into(),
        })?;
        let payload_size = key.len() + value.len();
        let old_size = self
            .memtable
            .size
            .fetch_add(payload_size, std::sync::atomic::Ordering::Relaxed);
        self.memtable
            .tree
            .write()
            .map_err(|_| DbError::LockPoisoned)?
            .insert(
                Bytes::copy_from_slice(key.as_bytes()),
                Bytes::copy_from_slice(value.as_bytes()),
            );
        if old_size + payload_size >= MAX_MEMTABLE_SIZE as usize {
            self.lock();
        }

        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Bytes>, DbError> {
        let map = self
            .memtable
            .tree
            .read()
            .map_err(|_| DbError::LockPoisoned)?;
        Ok(map
            .get(key.as_bytes())
            .cloned()
            .filter(|value| !value.is_empty()))
    }

    pub fn delete(&self, key: &str) -> Result<(), DbError> {
        self.wal.append(&WalEntry::Delete { key: key.into() })?;
        self.memtable
            .tree
            .write()
            .map_err(|_| DbError::LockPoisoned)?
            .insert(
                Bytes::copy_from_slice(key.as_bytes()),
                Bytes::copy_from_slice(&Bytes::new()),
            );
        Ok(())
    }

    pub fn lock(&self) -> Result<(), DbError> {
        let fresh = Memtable {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
            size: 0.into(),
        };
        let memtable = self.memtable.write().map_err(|_| DbError::LockPoisoned)?;
        let old = std::mem::replace(&mut memtable, fresh);
        self.locked_tables.push(old);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum WalEntry {
    Put { key: String, value: String },
    Delete { key: String },
}

// we use json for this wal bc i wanna read the contents
pub struct Wal {
    file: Mutex<fs::File>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
    fn append(&self, entry: &WalEntry) -> Result<(), DbError> {
        let entry = serde_json::to_string(entry)?;
        let mut file = self.file.lock().map_err(|_| DbError::LockPoisoned)?;

        file.write(entry.as_bytes())?;
        file.write_all(b"\n")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_value() -> Result<(), DbError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("wal.log");
        let db = Db::create(&path)?;

        db.put("20", "hi")?;

        let got = db.get("20")?;
        assert_eq!(got, Some(Bytes::from("hi")));
        Ok(())
    }

    #[test]
    fn put_then_delete_then_get_returns_nothing() -> Result<(), DbError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("wal.log");
        let db = Db::create(&path)?;

        db.put("20", "hi")?;
        db.delete("20")?;

        let got = db.get("20")?;
        assert_eq!(got, None);
        Ok(())
    }
    #[test]
    fn delete_does_note_crash() -> Result<(), DbError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("wal.log");
        let db = Db::create(&path)?;

        db.delete("20")?;
        Ok(())
    }
    #[test]
    fn get_not_found() -> Result<(), DbError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("wal.log");
        let db = Db::create(&path)?;

        assert_eq!(db.get("20")?, None);
        Ok(())
    }
    #[test]
    fn update_value() -> Result<(), DbError> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("wal.log");
        let db = Db::create(&path)?;

        db.put("20", "hi")?;
        assert_eq!(db.get("20")?, Some(Bytes::from("hi")));
        db.put("20", "bye")?;
        assert_eq!(db.get("20")?, Some(Bytes::from("bye")));
        Ok(())
    }
}
