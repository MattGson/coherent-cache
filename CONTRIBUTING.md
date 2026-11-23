# Contributing

Use the pinned Rust toolchain in `rust-toolchain.toml`. On Linux, install
`pkg-config` and the OpenSSL development headers for the native TLS dependencies.

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
cargo test --locked --doc --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --locked --no-deps --all-features
```

The ignored tests modify database schemas and terminate listener connections.
Use a disposable database and run them serially:

```sh
docker run --rm --detach --name coherent-cache-test \
  -e POSTGRES_PASSWORD=test-only -e POSTGRES_DB=cache_test \
  -p 127.0.0.1:55432:5432 postgres:17-alpine
export CACHE_TEST_DATABASE_URL=postgres://postgres:test-only@localhost:55432/cache_test
# Wait until `docker exec coherent-cache-test pg_isready -U postgres` succeeds.
cargo test --locked --lib -- --ignored --test-threads=1
docker stop coherent-cache-test
```

CI runs the suite on PostgreSQL 13 with Rust 1.88 and PostgreSQL 17 with stable
Rust. Keep `Cargo.lock` committed so tests and release verification are reproducible.
Consumers resolve their own dependency graph; the Rust 1.88 check covers the
committed lockfile.

Add regression coverage for changes to invalidation or migration behavior. Preserve
public SQL protocol names and migration helpers unless a documented migration is
part of the change. Historical application migrations may still call the legacy
generation helpers.

Unless explicitly stated otherwise, contributions are licensed under the same
MIT OR Apache-2.0 terms as the project.
