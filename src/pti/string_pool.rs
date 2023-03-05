use super::utils::Result;

use std::{collections::hash_map::DefaultHasher, hash::{Hash, Hasher}};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ref {
    begin: u32,
    end: u32,
}
impl Ref {
    pub fn is_empty(self) -> bool {
        self.end <= self.begin
    }

    pub fn num_bytes(self) -> usize {
        (self.end - self.begin) as usize
    }
}
impl Default for Ref {
    fn default() -> Self {
        Self {
            begin: 0,
            end: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Idx(u32);
impl Idx {
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn index(self) -> usize {
        assert!(!self.is_empty());
        self.0 as usize - 1
    }
}
impl Default for Idx {
    fn default() -> Self {
        Self(0)
    }
}

#[derive(Debug, Clone)]
struct MapEntry {
    r: Ref,
    hash: u64,
    idx: Idx,
}

#[derive(Debug)]
pub struct Pool {
    pool: Vec<u8>,
    map: Vec<MapEntry>,
    strings: Vec<Ref>,
}
impl Pool {
    fn make_map(size: usize) -> Vec<MapEntry> {
        std::iter::repeat(MapEntry {
            r: Ref::default(),
            hash: 0,
            idx: Idx(0),
        }).take(size).collect()
    }

    fn probe(&self, hash: u64) -> impl Iterator<Item=usize> {
        struct ProbeIter {
            map_size: usize,
            idx: usize,
            i: usize,
        }
        impl Iterator for ProbeIter {
            type Item = usize;
            fn next(&mut self) -> Option<Self::Item> {
                if self.i >= self.map_size / 2 {
                    None
                } else {
                    let idx = self.idx;
                    self.i += 1;
                    self.idx = (self.idx + self.i) % self.map_size;
                    Some(idx)
                }
            }
        }

        ProbeIter {
            map_size: self.map.len(),
            idx: (hash % self.map.len() as u64) as usize,
            i: 0,
        }
    }

    fn probe_first_empty(&mut self, hash: u64) -> usize {
        self.probe(hash).find(|&idx| self.map[idx].r.is_empty()).unwrap()
    }

    pub fn new() -> Self {
        Self {
            pool: Vec::new(),
            map: Self::make_map(1024),
            strings: [Ref::default()].into(),
        }
    }

    pub fn string_count(&self) -> usize {
        self.strings.len() - 1
    }

    pub fn get(&self, r: Ref) -> &str {
        let bytes = &self.pool[r.begin as usize..r.end as usize];

        // This always succeeds if `r` was produced by this pool, because only UTF-8 strings are ever inserted.
        // However, using from_utf8_unchecked would be incorrect because `r` may have been obtained from a different
        // pool.
        std::str::from_utf8(bytes).unwrap()
    }

    pub fn ref_by_idx(&self, idx: Idx) -> Ref {
        self.strings[idx.0 as usize]
    }

    pub fn put(&mut self, string: &str) -> Result<(Ref, Idx)> {
        if string.len() >= u32::MAX as usize {
            return Err("String is too long for string_pool::Pool".into())
        }
        if string.is_empty() {
            return Ok((Ref::default(), Idx(0)))
        }

        let mut hasher = DefaultHasher::new();
        string.hash(&mut hasher);
        let hash = hasher.finish();

        let mut found_idx = None;
        for probe_idx in self.probe(hash) {
            let e = &self.map[probe_idx];
            if e.r.is_empty() {
                found_idx = Some(probe_idx);
                break
            }
            if e.hash == hash {
                if self.get(e.r) == string {
                    return Ok((e.r, e.idx))
                }
            }
        }

        if string.len() >= u32::MAX as usize - self.pool.len() ||
           self.strings.len() >= u32::MAX as usize - 1 {
            return Err("string_pool::Pool overflow".into())
        }

        let map_idx =
            if found_idx.is_none() || self.strings.len() >= self.map.len() / 8 * 5 {
                // Grow the map.
                let new_map_size = self.map.len() / 8 * 11;
                for entry in std::mem::replace(&mut self.map, Self::make_map(new_map_size)) {
                    if entry.r.is_empty() {
                        continue
                    }

                    let idx = self.probe_first_empty(entry.hash);
                    self.map[idx] = entry;
                }

                // Find the entry in the grown map.
                self.probe_first_empty(hash)
            } else {
                found_idx.unwrap()
            };

        let begin = self.pool.len() as u32;
        self.pool.extend_from_slice(string.as_bytes());
        let r = Ref {
            begin,
            end: self.pool.len() as u32,
        };

        let idx = Idx(self.strings.len() as u32);
        self.strings.push(r);
        self.map[map_idx] = MapEntry {
            r,
            hash,
            idx,
        };
        Ok((r, idx))
    }
}
