# gurafu

A hexastore implementation using RocksDB.

**It's just for practice and not for production**

## Hexastore

A triple `a -[b]-> c` is coded as below:

```rust
Triple("a".into(), "b".into(), "c".into())
```

To get the all triples which connected by the predicate of `b`, you can make the query as shown as below:

```rust
CriteriaBuilder::default()
    .predicate("b".into())
    .build()
```
