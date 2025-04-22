# delta-rs-mt
`delta-rs-mt` is a proof-of-concept fork of `delta-rs` (see original README [here](README.delta-rs.md)) that supports transactions with *multi-table scope*, without the use of an external commit coordinator. `delta-rs-mt` does this by introducing *Transaction Groups* (or T-Groups) - a group of tables that share commit logs. Any transaction updating tables in a T-group atomically commits changes for all tables, or none of them.

## Installation
Follow these steps to build the project, and set it up to run test and benchmarks.

### 1. Install Rust

On Linux and macOS systems, this is done as follows:
```sh
curl https://sh.rustup.rs -sSf | sh
```

### 2. Install the `uv` Python package manager
On Linux and macOS systems, this is done as follows:
```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3. Build the project
Build the project. This will install deltalake into the Python virtual environment managed by uv.
```sh
cd python
make develop
```

## Benchmark Setup & Execution
Follow these steps to generate TPC‑DS data, convert it into Delta tables, and run various benchmarks.

### 1. Generate TPC‑DS Data

```bash
# 1.1 Clone the TPC‑DS kit
cd Code_Delta/
git clone https://github.com/databricks/tpcds-kit.git

# 1.2 Build the dsdgen tool
cd tpcds-kit/tools

make

# 1.3 Generate a scale‑10 dataset
./dsdgen -scale 10 -FORCE -dir /home/divyams/Code_Delta/tpcds-data

```
 #### If you encounter compilation errors with gcc, switch your compiler to clang:

 ```bash
 export CC=clang
 export CXX=clang++
 ```

## 2. Convert TPC‑DS Data to Delta Tables

Make a new directory tpcds-delta for storing converted tables.

```bash
cargo run --release --bin merge_write -- convert ./tpcds-data/web_returns_edit.dat ./tpcds-delta/web_returns
```

## 3. Run Benchmarks
### 3.1 Read Benchmark

```bash
  cargo run --release --bin merge_write -- read-perf ./tpcds-delta/web_returns
```

### 3.2 Write Benchmark
#### 3.2.1 Single Table

```bash
cargo run --release --bin merge_write -- write-perf ./tpcds-delta/merge_results_2 <num_rows>
```
#### 3.2.2 Multi Table Write (2 Tables Parallel Txn)
<mark> Note: Make sure correct tables path in merge_write under Command: WriteMultiTableTGroup</mark>

For Delta-RS-MT (with TGroups)
```bash
cargo run --release --bin merge_write -- write-multi-table-t-group <num_rows>
```


<mark>Note: Make sure correct tables path in merge_write under Command: WriteMultiTable</mark>

For Delta-RS (non TGroups)

```bash
cargo run --release --bin merge_write -- write-multi-table <num_rows>
```
