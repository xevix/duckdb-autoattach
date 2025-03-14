# Autoattach DuckDB Extension

WIP

NOTE: Requires nightly/main for ATTACH OR REPLACE feature currently targeting DuckDB v1.3

This extension watches directories for DuckDB files and automatically attaches and replaces a database alias with the latest file. This lets a webserver or app with a long-running DuckDB instance easily update to the latest version of a file produced upstream.

Internally EFSW is used to watch local files, and the `ATTACH OR REPLACE` functionality is used to atomically replace databases.

If S3 URL is given, S3 is polled at an interval for newer file to autoattach. In the future this might listen for S3 events to be in line with how EFSW works locally.

- [X] S3 watcher
- [X] Filesystem watcher
- [X] Attach latest file on first call to attach_auto()
- [ ] Glob pattern matching for watched dirs
- [ ] Input validation to attach_auto()

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

## Install

For now it must be built from source using the standard DuckDB build mechanism.

```
$ GEN=ninja make
# For S3 support
CORE_EXTENSIONS='httpfs;parquet' GEN=ninja make
```

## Usage

```
-- ATTACH using alias 'watched_db' files matching the given pattern
SELECT attach_auto('watched_db', '/path/to/watch/*.duckdb');
-- Or on an S3 path. Requires S3 querying setup in DuckDB
SET s3_poll_interval = 5; -- Optionally set poll interval (in seconds)
SELECT attach_auto('s3_db', 's3://my-bucket/*.duckdb');

-- Query as usual
FROM watched_db.tbl;

-- ... newer file added to watched directory ...

-- Continue querying as usual, but will have latest data
FROM watched_db.tbl;

-- Stop watching and detach when done
SELECT detach_auto('watched_db');

```

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/autoattach/autoattach.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `autoattach.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `autoattach()` that takes a string arguments and returns a string:
```
D select autoattach('Jane') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Autoattach Jane 🐥 │
└───────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL autoattach
LOAD autoattach
```
