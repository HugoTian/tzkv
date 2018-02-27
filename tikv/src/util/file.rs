// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{self, ErrorKind, Read};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use crc::crc32::{self, Digest, Hasher32};

pub fn get_file_size(path: &PathBuf) -> io::Result<u64> {
    let meta = fs::metadata(path)?;
    Ok(meta.len())
}

pub fn file_exists(file: &PathBuf) -> bool {
    let path = Path::new(file);
    path.exists() && path.is_file()
}

pub fn delete_file_if_exist(file: &PathBuf) {
    match fs::remove_file(file) {
        Ok(_) => {}
        Err(ref e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => {
            warn!("failed to delete file {}: {:?}", file.display(), e);
        }
    }
}

pub fn copy_and_sync<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<u64> {
    if !from.as_ref().is_file() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "the source path is not an existing regular file",
        ));
    }

    let mut reader = File::open(from)?;
    let mut writer = File::create(to)?;

    let res = io::copy(&mut reader, &mut writer)?;
    writer.sync_all()?;
    Ok(res)
}

const DIGEST_BUFFER_SIZE: usize = 1024 * 1024;

pub fn calc_crc32<P: AsRef<Path>>(path: P) -> io::Result<u32> {
    let mut digest = Digest::new(crc32::IEEE);
    let mut f = OpenOptions::new().read(true).open(path)?;
    let mut buf = vec![0; DIGEST_BUFFER_SIZE];
    loop {
        match f.read(&mut buf[..]) {
            Ok(0) => {
                return Ok(digest.sum32());
            }
            Ok(n) => {
                digest.write(&buf[..n]);
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use std::fs::OpenOptions;
    use tempdir::TempDir;
    use rand::{thread_rng, Rng};

    use super::*;

    #[test]
    fn test_get_file_size() {
        let tmp_dir = TempDir::new("").unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        // Ensure it works to get the size of an empty file.
        let empty_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&empty_file)
                .unwrap();
        }
        assert_eq!(get_file_size(&empty_file).unwrap(), 0);

        // Ensure it works to get the size of an non-empty file.
        let non_empty_file = dir_path.join("non_empty_file");
        let size = 5;
        let v = vec![0; size];
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&non_empty_file)
                .unwrap();
            f.write_all(&v[..]).unwrap();
        }
        assert_eq!(get_file_size(&non_empty_file).unwrap(), size as u64);

        // Ensure it works for non-existent file.
        let non_existent_file = dir_path.join("non_existent_file");
        assert!(get_file_size(&non_existent_file).is_err());
    }

    #[test]
    fn test_file_exists() {
        let tmp_dir = TempDir::new("").unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        assert_eq!(file_exists(&dir_path), false);

        let existent_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&existent_file)
                .unwrap();
        }
        assert_eq!(file_exists(&existent_file), true);

        let non_existent_file = dir_path.join("non_existent_file");
        assert_eq!(file_exists(&non_existent_file), false);
    }

    #[test]
    fn test_delete_file_if_exist() {
        let tmp_dir = TempDir::new("").unwrap();
        let dir_path = tmp_dir.path().to_path_buf();

        let existent_file = dir_path.join("empty_file");
        {
            let _ = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&existent_file)
                .unwrap();
        }
        assert_eq!(file_exists(&existent_file), true);
        delete_file_if_exist(&existent_file);
        assert_eq!(file_exists(&existent_file), false);

        let non_existent_file = dir_path.join("non_existent_file");
        delete_file_if_exist(&non_existent_file);
    }

    fn gen_rand_file<P: AsRef<Path>>(path: P, size: usize) -> u32 {
        let s: String = thread_rng().gen_ascii_chars().take(size).collect();
        File::create(path).unwrap().write_all(s.as_bytes()).unwrap();
        let mut digest = Digest::new(crc32::IEEE);
        digest.write(s.as_bytes());
        digest.sum32()
    }

    #[test]
    fn test_calc_crc32() {
        let tmp_dir = TempDir::new("").unwrap();

        let small_file = tmp_dir.path().join("small.txt");
        let small_checksum = gen_rand_file(&small_file, 1024);
        assert_eq!(calc_crc32(&small_file).unwrap(), small_checksum);

        let large_file = tmp_dir.path().join("large.txt");
        let large_checksum = gen_rand_file(&large_file, DIGEST_BUFFER_SIZE * 4);
        assert_eq!(calc_crc32(&large_file).unwrap(), large_checksum);
    }
}
