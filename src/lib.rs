use bytes::Bytes;
use std::{
    collections::BTreeMap,
    fs::{self},
    io::{self, Read, Write},
    sync::{Arc, RwLock},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("io error")]
    Io(#[from] io::Error),
    #[error("serde error")]
    Serialize(#[from] serde_json::Error),
    #[error("database lock was poisoned")]
    LockPoisoned,
}

pub struct DB {
    map: Arc<RwLock<BTreeMap<Bytes, Bytes>>>,
    wal: WAL,
}

impl DB {
    pub fn create(log_path: String) -> Result<Self, DBError> {
        let map = Arc::new(RwLock::new(BTreeMap::new()));
        let wal = WAL::create(log_path)?;
        Ok(Self { map, wal })
    }
    pub fn fill(&mut self) -> Result<(), DBError> {
        let mut content = String::new();
        self.wal.file.read_to_string(&mut content)?;
        let mut map = self.map.write().map_err(|_| DBError::LockPoisoned)?;
        content.split('\n').for_each(|line| {
            if let Some((key, value)) = line.split_once(',') {
                map.insert(
                    Bytes::copy_from_slice(key.as_bytes()),
                    Bytes::copy_from_slice(value.as_bytes()),
                );
            }
        });
        Ok(())
    }

    pub fn put(&mut self, key: &str, value: &str) -> Result<(), DBError> {
        self.wal.write(key, value)?;
        self.map.write().map_err(|_| DBError::LockPoisoned)?.insert(
            Bytes::copy_from_slice(key.as_bytes()),
            Bytes::copy_from_slice(value.as_bytes()),
        );
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Bytes>, DBError> {
        let map = self.map.read().map_err(|_| DBError::LockPoisoned)?;
        Ok(map
            .get(key.as_bytes())
            .cloned()
            .filter(|value| !value.is_empty()))
    }

    pub fn delete(&mut self, key: &str) -> Result<(), DBError> {
        self.put(key, "")?;
        Ok(())
    }
}

pub struct WAL {
    pub file: fs::File,
}

impl WAL {
    pub fn create(path: String) -> Result<Self, DBError> {
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self { file })
    }
    pub fn write(&mut self, key: &str, value: &str) -> Result<(), DBError> {
        self.file
            .write_all(&format!("{key},{value}\n").into_bytes())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_returns_value() -> Result<(), DBError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let mut db = DB::create(path.to_string_lossy().into_owned())?;

        db.put("20", "hi")?;

        let got = db.get("20")?;
        assert_eq!(got, Some(Bytes::from("hi")));
        Ok(())
    }
    
    #[test]
    fn put_then_delete_then_get_returns_nothing() -> Result<(), DBError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let mut db = DB::create(path.to_string_lossy().into_owned())?;

        db.put("20", "hi")?;
        db.delete("20")?;

        let got = db.get("20")?;
        assert_eq!(got, None);
        Ok(())
    }
        #[test]
    fn delete_does_note_crash() -> Result<(), DBError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let mut db = DB::create(path.to_string_lossy().into_owned())?;

        db.delete("20")?;
        Ok(())
    }
        #[test]
    fn get_not_found() -> Result<(), DBError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let db = DB::create(path.to_string_lossy().into_owned())?;

        assert_eq!(db.get("20")?, None);
        Ok(())
    }
        #[test]
    fn update_value() -> Result<(), DBError> {
        let path = std::env::temp_dir().join("lsm-test-wal.log");
        let mut db = DB::create(path.to_string_lossy().into_owned())?;

        db.put("20", "hi")?;
        assert_eq!(db.get("20")?, Some(Bytes::from("hi")));
        db.put("20","bye")?;
        assert_eq!(db.get("20")?, Some(Bytes::from("bye")));
        Ok(())
    }


}
