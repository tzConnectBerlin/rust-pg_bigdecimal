# rust-pg_bigdecimal

A Rust datatype implementation for the Postgres Numeric type (ie the types listed in https://www.postgresql.org/docs/13/datatype-numeric.html under "decimal"/"numeric"), to be used with Rust's "Postgres" library.
The full spectrum of Postgres' Numeric value range is supported.

This small Rust package has been created because currently the main "Postgres" library (https://docs.rs/postgres/0.19.1/postgres/index.html) does not provide a native datatype to read/write Numeric values.

We only implement the wire logic of Postgres' Numeric datatype. We didn't rewrite the whole logic of big number manipulation,
rather we let that logic be implemented by the already popular BigDecimal package (https://docs.rs/bigdecimal/0.3.0/bigdecimal/).

Specifically, our new Rust datatype `PgNumeric` is simply an Optional `BigDecimal`.
With `None` representing the Postgres Numeric value `NaN`, and all `Some(..)` representing Postgres Numeric numbers.

Listed on crates.io here: https://crates.io/crates/pg_bigdecimal.

## Comparisons between similar packages

- https://crates.io/crates/rust-decimal provides a rust native type, however it's represented as a 96 bit integer number + scaling bits + 1 sign bit.
This means that "only" (quoting only here because it is still a large integer space) a small part of Postgres' Numeric values can be translated. In our case it was not sufficient.
