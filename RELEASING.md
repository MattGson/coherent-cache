# Releasing

The repository is prepared for crates.io but has not been published. The initial
version is `0.1.0`. A dry run does not reserve the crate name.

## First publication

1. The public repository is already hosted at
   https://github.com/MattGson/coherent-cache. Push release preparation commits to `main`.

2. Confirm the repository URL in `[package]` matches the remote you created:

   ```toml
   repository = "https://github.com/MattGson/coherent-cache"
   ```

3. Confirm the package name is still available on crates.io, the license choice is
   correct, and your crates.io account has a verified email address.
4. For the manual GitHub workflow, configure an environment named `crates-io`
   and its `CARGO_REGISTRY_TOKEN` secret with permission to publish this crate.
   Use environment protection rules if release approval is desired. Never commit
   the token or place it in an issue or pull request.

## Every release

1. Set the intended version in `Cargo.toml`, update `Cargo.lock` with `cargo check`,
   and finalize the corresponding `CHANGELOG.md` entry. For the first release, update
   the README installation section to use the registry dependency.
2. Run the checks in `CONTRIBUTING.md`, including the disposable PostgreSQL suite.
3. Inspect and verify the archive:

   ```sh
   cargo package --list --locked
   cargo publish --dry-run --locked
   ```

   The explicit package allowlist contains only Rust source, the example, manifests,
   lockfile, README, changelog, and licenses. CI and release instructions remain in Git.
4. Commit and push to `main`. Dispatch **Publish crate** on `main`, entering the exact
   manifest version. It reruns CI before publishing. Alternatively, authenticate
   locally with `cargo login` and run `cargo publish --locked` from the verified commit.
5. After a successful publication, tag that exact commit and push the tag:

   ```sh
   git tag -a v0.1.0 -m 'Release 0.1.0'
   git push origin v0.1.0
   ```

The workflow does not publish on pushes or tags. Published crate versions cannot
be overwritten; correct errors in a new version. See the
[Cargo publishing guide](https://doc.rust-lang.org/cargo/reference/publishing.html).
