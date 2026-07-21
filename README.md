# rbatis-cache

`rbatis-cache` defines the second-level cache SPI shared by
`rbatis-moka`, `rbatis-redis`, and `rbatis-memcached`.

Implemented invariants:

- cache only parsed `SELECT` statements outside transactions;
- BLAKE3 keys isolate version, data source, driver, tenant, namespace,
  statement ID, generation, canonical SQL, and encoded parameters;
- MessagePack envelopes and SQL-parser-derived table tags;
- namespace generation invalidation without key scans;
- per-key singleflight loading;
- backend errors fail open to the database loader and remain observable;
- cached bytes represent database/encrypted state, before verification or decryption.

This is an alpha contract. The RBatis executor integration (via the
`rbatis::intercept::Intercept` trait) and distributed backends are developed
in their own repositories.