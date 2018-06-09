extern crate rocksdb;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
use std::ops::Index;
use std::path::Path;
use std::borrow::Cow;
use std::io::Write;

#[derive(Serialize, Deserialize, Debug)]
pub struct Triple<'a>(Cow<'a, str>, Cow<'a, str>, Cow<'a, str>);

impl<'a> Index<Property> for Triple<'a> {
    type Output = Cow<'a, str>;

    fn index(&self, property: Property) -> &Self::Output {
        use Property::*;
        match property {
            Subject => &self.0,
            Predicate => &self.1,
            Object => &self.2,
        }
    }
}

pub struct ReorderedTriple<'a>(Cow<'a, str>, Cow<'a, str>, Cow<'a, str>);
impl<'a> ReorderedTriple<'a> {
    fn to_key(&self) -> Vec<u8> {
        let ReorderedTriple(a, b, c) = self;
        let mut buf = Vec::with_capacity(a.len() + b.len() + c.len() + 2);
        buf.write(a.as_bytes()).unwrap();
        buf.write(&[0xFF]).unwrap();
        buf.write(b.as_bytes()).unwrap();
        buf.write(&[0xFF]).unwrap();
        buf.write(c.as_bytes()).unwrap();
        buf.write(&[0xFF]).unwrap();
        buf
    }
}

impl<'a> Triple<'a> {
    fn reorder(&self, order: &PropertyOrder) -> ReorderedTriple<'a> {
        let a = &self[order.0];
        let b = &self[order.1];
        let c = &self[order.2];
        ReorderedTriple(a.clone(), b.clone(), c.clone())
    }

    fn to_value(&self) -> Vec<u8> {
        bincode::serialize(self).expect("serialized triple")
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Property {
    Subject,
    Predicate,
    Object,
}

pub type PropertyOrder = (Property, Property, Property);

const HEXAGON: [(PropertyOrder, &'static str); 6] = {
    use Property::{Subject as S, Predicate as P, Object as O};
    [
        ((S, P, O), "spo"),
        ((S, O, P), "sop"),
        ((P, O, S), "pos"),
        ((P, S, O), "pso"),
        ((O, S, P), "osp"),
        ((O, P, S), "ops"),
    ]
};

const HEXAGON_BITS: [u16; 6] = [
    0b_100_010_001,
    0b_100_001_010,
    0b_001_100_010,
    0b_010_100_001,
    0b_010_001_100,
    0b_001_010_100,
];

const SIZE_MASK_MAP: u32 = 0b_0111_0110_0110_0100_0110_0100_0100_0000;

#[derive(Default)]
pub struct CriteriaBuilder<'a> {
    subject: Option<Cow<'a, str>>,
    predicate: Option<Cow<'a, str>>,
    object: Option<Cow<'a, str>>,
    flags: u8,
}

impl<'a> CriteriaBuilder<'a> {
    pub fn subject(mut self, subject: Cow<'a, str>) -> Self {
        self.subject = Some(subject);
        self.flags = self.flags | 0b_100;
        return self;
    }

    pub fn predicate(mut self, predicate: Cow<'a, str>) -> Self {
        self.predicate = Some(predicate);
        self.flags = self.flags | 0b_010;
        return self;
    }

    pub fn object(mut self, object: Cow<'a, str>) -> Self {
        self.object = Some(object);
        self.flags = self.flags | 0b_001;
        return self;
    }

    pub fn build(self) -> Criteria<'a> {
        let f = self.flags as u16;
        let size_mask: u16 = ((SIZE_MASK_MAP >> (f * 4)) & 0b_0111) as u16;
        let fat_flags
            = (f & 0b100) * 0b_111_000_0
            | (f & 0b010) * 0b_111_00
            | (f & 0b001) * 0b_111
        ;
        let mask = ((size_mask << 6) | (size_mask << 3) | size_mask) | (!fat_flags & 0b111111111);

        let CriteriaBuilder{
            subject,
            predicate,
            object,
            flags: _,
        } = self;

        Criteria {
            subject,
            predicate,
            object,
            mask,
        }
    }
}

#[derive(Debug, Default)]
pub struct Criteria<'a> {
    subject: Option<Cow<'a, str>>,
    predicate: Option<Cow<'a, str>>,
    object: Option<Cow<'a, str>>,
    mask: u16,
}

impl<'a> Index<Property> for Criteria<'a> {
    type Output = Option<Cow<'a, str>>;

    fn index(&self, property: Property) -> &Self::Output {
        use Property::*;
        match property {
            Subject => &self.subject,
            Predicate => &self.predicate,
            Object => &self.object,
        }
    }
}

impl<'a> Criteria<'a> {
    pub fn usable_indices(&self) -> UsableIndices {
        UsableIndices { idx: 0, mask: self.mask }
    }

    pub fn prefix(&self, order: &PropertyOrder) -> Vec<u8> {
        let mut prefix = Vec::new();
        if let Some(a) = &self[order.0] {
            prefix.write(a.as_bytes()).unwrap();
            prefix.write(&[0xFF]).unwrap();
        }
        if let Some(b) = &self[order.1] {
            prefix.write(b.as_bytes()).unwrap();
            prefix.write(&[0xFF]).unwrap();
        }
        if let Some(c) = &self[order.2] {
            prefix.write(c.as_bytes()).unwrap();
            prefix.write(&[0xFF]).unwrap();
        }
        prefix.write(&[0xFF]).unwrap();
        prefix
    }
}

pub struct UsableIndices {
    idx: usize,
    mask: u16,
}

impl<'a> Iterator for UsableIndices {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        while self.idx < HEXAGON_BITS.len() {
            let idx = self.idx;
            let bits = HEXAGON_BITS[idx];
            self.idx += 1;
            if (bits & self.mask) == bits {
                return Some(idx);
            }
        }
        None
    }
}

type CFHandles = [rocksdb::ColumnFamily; 6];

pub struct Hexastore {
    db: rocksdb::DB,
    cf_handles: CFHandles,
}

impl Hexastore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Hexastore, rocksdb::Error> {
        let mut opts = rocksdb::Options::default();
        opts.create_missing_column_families(true);
        opts.create_if_missing(true);
        let cfs = HEXAGON.iter().map(|(_, name)| {
            let cf_opts = rocksdb::Options::default();
            rocksdb::ColumnFamilyDescriptor::new(name.to_string(), cf_opts)
        }).collect();
        let db = rocksdb::DB::open_cf_descriptors(&opts, path, cfs)?;
        let mut cf_handles: CFHandles = unsafe { std::mem::uninitialized() };
        for (&(_, name), cf) in HEXAGON.iter().zip(cf_handles.iter_mut()) {
            *cf = db.cf_handle(&name).expect("CF Handle");
        }
        Ok(Hexastore { db, cf_handles })
    }

    pub fn put<'a>(&self, triple: Triple<'a>) {
        let mut write_batch = rocksdb::WriteBatch::default();
        for ((ord, _), cf) in HEXAGON.iter().zip(self.cf_handles.iter()) {
            let reordered = triple.reorder(ord);
            let key = reordered.to_key();
            let value = triple.to_value();
            write_batch.put_cf(cf.clone(), &key, &value).expect("crate put operation");
        }
        self.db.write(write_batch).expect("complete to put");
    }

    pub fn get(&self, criteria: &Criteria) -> Result<Get, rocksdb::Error> {
        let idx = criteria.usable_indices().next().expect("usable index");
        let cf = self.cf_handles[idx].clone();
        let (ord, _) = HEXAGON[idx];
        let prefix = criteria.prefix(&ord);
        let mut opt = rocksdb::ReadOptions::default();
        opt.set_iterate_upper_bound(&prefix);
        let mode = rocksdb::IteratorMode::From(&prefix[..prefix.len() - 1], rocksdb::Direction::Forward);
        let inner = self.db.iterator_cf_opt(cf, mode, &opt)?;
        Ok(Get { inner })
    }
}

pub struct Get {
    inner: rocksdb::DBIterator,
}

impl Iterator for Get {
    type Item = Triple<'static>;

    fn next(&mut self) -> Option<Self::Item> {
        let value = if let Some((_, value)) = self.inner.next() {
            value
        } else {
            return None;
        };
        let tri: Triple = bincode::deserialize(&value).unwrap();
        Some(tri)
    }
}

fn main() {
    let db = Hexastore::new("./tmp.db").expect("hexastore");
    db.put(Triple("a".into(), "b".into(), "c".into()));
    db.put(Triple("a".into(), "b".into(), "c2".into()));
    db.put(Triple("d".into(), "b".into(), "c".into()));
    db.put(Triple("e".into(), "b".into(), "f".into()));
    db.put(Triple("e".into(), "b".into(), "c2".into()));
    let criteria = CriteriaBuilder::default()
        .object("b".into())
        .build();
    let results: Vec<_> = db.get(&criteria).expect("result").collect();
    println!("{:?}", results);
}
