# delta-rs-mt
`delta-rs-mt` is a proof-of-concept fork of `delta-rs` (see original README [here](README.delta-rs.md)) that supports transactions with *multi-table scope*, without the use of an external commit coordinator. `delta-rs-mt` does this by introducing *Transaction Groups* (or T-Groups) - a group of tables that share commit logs. Any transaction updating tables in a T-group atomically commits changes for all tables, or none of them.

## Installation
Follow these steps to build the project, and set it up to run test and benchmarks.

### 1. Clone this repo
```sh
git clone https://github.com/naveen-u/delta-rs-mt.git
```

### 2. Install Rust

On Linux and macOS systems, this is done as follows:
```sh
curl https://sh.rustup.rs -sSf | sh
```

### 3. Install the `uv` Python package manager
On Linux and macOS systems, this is done as follows:
```sh
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 4. Build the project
Build the project. This will install deltalake into the Python virtual environment managed by uv.
```sh
cd python
make develop
```

## Benchmark Setup & Execution
Follow these steps to generate TPC‑DS data, convert it into Delta tables, and run various benchmarks.

### 1. Generate TPC‑DS Data

#### 1.1 Clone the TPC-DS kit
```bash
git clone https://github.com/databricks/tpcds-kit.git
```

#### 1.2 Install necessary development tools
Ubuntu:
```sh
sudo apt-get install gcc make flex bison byacc git
```
CentOS/RHEL:
```sh
sudo yum install gcc make flex bison byacc git
```
macOS
```sh
xcode-select --install
```

#### 1.3 Build the dsdgen tool
```sh
cd tpcds-kit/tools
make
```

#### 1.4 Generate a scale‑10 dataset
```sh
export TPCDS_DATA_PATH=/tmp/tpcds-data   # Set this path to wherever TPC-DS data is to be created
mkdir -p $TPCDS_DATA_PATH
./dsdgen -scale 10 -FORCE -dir $TPCDS_DATA_PATH
```

> [!TIP]
> If you encounter compilation errors with `gcc`, switch your compiler to `clang` and retry.
```sh
 export CC=clang
 export CXX=clang++
```

### 2. Convert TPC‑DS Data to Delta Tables
Note that `delta-rs` (and hence, `delta-rs-mt`) contains schema only for the web returns dataset and hence, we test using that dataset. For testing with multiple tables, multiple copies of the table can be created by re-running these commands with a different table path.

#### 2.1 Clean up TPC-DS data file
The generated TPC-DS data has an extra `|` delimiter at the end of each line. This messes with the delta table generation script in `delta-rs`. Clean that up first using:
```sh
sed 's/|$//' $TPCDS_DATA_PATH/web_returns.dat > $TPCDS_DATA_PATH/web_returns_edit.dat
```

#### 2.2 Generate delta tables
Generate the delta table from TPC-DS web returns data using the script provided by `delta-rs`. Run the following from the root of the `delta-rs` (or `delta-rs-mt`) repo.
```sh
cd crates/benchmarks/src/bin
export TPCDS_TABLE_PATH=/tmp/tpcds-delta/web_returns    # Set this path to wherever the Delta table is to be created
cargo run --release --bin merge_write -- convert $TPCDS_DATA_PATH/web_returns_edit.dat $TPCDS_TABLE_PATH
```
> [!TIP]
> The web returns dataset with a scale factor of `10` contains `719217` rows. Depending on the memory of the system, the convert command above might get OOM killed. If so, either reduce the scale factor, or truncate the data file using:
> ```sh
> head -n 10000 $TPCDS_DATA_PATH/web_returns_edit.dat > $TPCDS_DATA_PATH/web_returns_trunc.dat    # Change 10000 to required number of rows
> mv $TPCDS_DATA_PATH/web_returns_trunc.dat $TPCDS_DATA_PATH/web_returns_edit.dat
> ```

Note that the generated tables do not have checkpoints created by default and are not part of any T-Group. The `delta-rs-mt` benchmark script contains utility commands to do both of these operations when necessary. Refer [Section 3.1](#31-utility-methods) below for their usage.

## 3. Run Benchmarks
### 3.1 Utility methods
The delta table's created in [Section 2.2](#22-generate-delta-tables) by default are not checkpointed and do not belong to any T-Group. The following utility methods are available to do these operations for generating tables suitable for testing.

#### 3.1.1 Checkpointing
> [!WARN]
> Manually checkpointing tables that are a part of a T-Group is not (yet) supported!

To manually checkpoint a delta table, run:
```sh
# Replace $TPCDS_TABLE_PATH with any delta table's path
cargo run --release --bin merge_write -- checkpoint $TPCDS_TABLE_PATH
```
#### 3.1.2 Create T-Group
To create a new T-Group, run:
```sh
# Replace $TGROUP_PATH with any path
cargo run --release --bin merge_write -- create-tgroup $TGROUP_PATH
```
This step simply creates a new directory with a `_delta_log` folder and an empty inital log file signaling the initialization of the T-Group (which can be used for T-Group metadata in the future).

#### 3.1.3 Add to T-Group
To add a delta table to a T-Group, run:
```sh
# Replace $TPCDS_TABLE_PATH with any delta table's path and $TGROUP_PATH with any T-Group's path
cargo run --release --bin merge_write -- add-to-tgroup $TPCDS_TABLE_PATH $TGROUP_PATH
```

### 3.2 Read Benchmarks

```bash
# Replace $TPCDS_TABLE_PATH with any delta table's path
cargo run --release --bin merge_write -- read-perf $TPCDS_TABLE_PATH
```

### 3.3 Write Benchmark
#### 3.3.1 Single Table

```bash
cargo run --release --bin merge_write -- write-perf ./tpcds-delta/merge_results_2 <num_rows>
```
#### 3.3.2 Multi Table Write (2 Tables Parallel Txn)
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

### 3.4 Multi-Table T-Group Write Example

This example shows how to perform an atomic, multi-table write (T-Group) in Rust using `delta-rs-mt`.

#### 3.4.1 Example Code
1. **Open your table** (must have a checkpoint):
   ```rust
   let table = deltalake::open_table("s3://my-bucket/my_table").await?;
   ```

2. **Prepare an Arrow `RecordBatch` (matching your table schema).** 

4. **Write in one step** (atomic, OCC-safe):
   ```rust
   use deltalake::DeltaOps;
   DeltaOps(table).write(vec![batch]).await?;
   ```

#### 4.2 Multi-Table T-Group Write

1. **Open each table** in your transaction group:
   ```rust
   let t1 = deltalake::open_table("s3://my-bucket/table1").await?;
   let t2 = deltalake::open_table("s3://my-bucket/table2").await?;
   ```

2. **Issue pre-commits** (no log append yet):
   ```rust
   let p1 = DeltaOps(t1.clone()).write_tgroup(vec![batch1]).get_precommit().await?;
   let p2 = DeltaOps(t2.clone()).write_tgroup(vec![batch2]).get_precommit().await?;
   ```

3. **(Optional) Tag & merge** each `PreCommit` with its `table_id`:
   ```rust
   let merged = combine_precommits_with_table_id(vec![p1, p2], vec![uuid1, uuid2])?;
   ```

4. **Final atomic multi-table commit:**
   ```rust
   let result = merged.await?;
   ```


## Source Code Explanation

### Benchmarks

#### 1. Read‐Only Benchmark

- **`async fn benchmark_read_tpcds`**  
  - **CLI**: `ReadPerf <path>`  
  - **Signature**:  
    ```rust
    async fn benchmark_read_tpcds(
      path: String
    ) -> Result<(Duration,ReadMetrics),DataFusionError>
    ```  
  - **Location**: `crates/benchmarks/src/bin/merge_write.rs`, lines **393–451**

  Although your focus was on writes, we also include ReadPerf (triggered via the ReadPerf CLI). Defined around line 350, it loads the Delta table, runs SELECT * FROM t1, collects all batches, sums row counts, and times the full scan+deserialize pipeline. Metrics are logged the same way into “data/benchmarks.”

---

#### 2.Write‐Only Benchmarks

- **`async fn benchmark_write_tpcds`**  
  - **CLI**: `WritePerf <path> <num_rows>`  
  - **Signature**:  
    ```rust
    async fn benchmark_write_tpcds(
      path: String,
      num_rows: usize
    ) -> Result<(Duration,WriteMetrics),DataFusionError>
    ```  
  - **Location**: `crates/benchmarks/src/bin/merge_write.rs`, lines **360–410**

  Invoked via the WritePerf CLI command, this async function lives starting around line 400 in src/main.rs. It loads the existing Delta table schema with DeltaTableBuilder::from_uri(..).load(), synthesizes a dummy RecordBatch of num_rows via create_dummy_record_batch(), appends it via DeltaOps::write(..), and measures total elapsed time with tokio::time::Instant. Results (row count + duration) are then appended into the “data/benchmarks” Delta table for later analysis.

- **`async fn benchmark_write_tpcds_tgroup`**  
  - **CLI**: `WriteTGroup <path> <num_rows>`  
  - **Signature**:  
    ```rust
    async fn benchmark_write_tpcds_tgroup(
      path: String,
      num_rows: usize
    ) -> Result<(Duration,WriteMetrics),DataFusionError>
    ```  
  - **Location**: `crates/benchmarks/src/bin/merge_write.rs`, lines **598–664**

  Accessible via the WriteTGroup CLI subcommand (alias of WritePerf), this variant appears just below benchmark_write_tpcds. Instead of a fire-and-forget write, it calls .get_precommit().await on the WriteBuilder, measures only the commit phase of the single-table transaction group, and then logs duration/metrics identically to WritePerf.

- **`async fn benchmark_write_tpcds_mt`**  
  - **CLI**: `WriteMultiTable <num_rows>`  
  - **Signature**:  
    ```rust
    async fn benchmark_write_tpcds_mt(
      table_paths: Vec<&str>,
      num_rows: usize
    ) -> Result<(Duration,WriteMetrics),DataFusionError>
    ```  
  - **Location**: `crates/benchmarks/src/bin/merge_write.rs`, lines **460–520**

  Mapped to the WriteMultiTable subcommand (around line 480), this routine accepts a list of independent table paths. It spawns one thread per table, each generating its own dummy batch and calling DeltaOps::write(..). The wall-clock time from before the first write to after all threads join captures parallel write throughput across multiple tables not in a transaction group.

- **`async fn benchmark_write_tpcds_tgroup_mt`**  
  - **CLI**: `WriteMultiTableTGroup <num_rows>`  
  - **Signature**:  
    ```rust
    async fn benchmark_write_tpcds_tgroup_mt(
      table_paths: Vec<&str>,
      num_rows: usize
    ) -> Result<(Duration,WriteMetrics),DataFusionError>
    ```  
  - **Location**: `crates/benchmarks/src/bin/merge_write.rs`, lines **520–580**

  Invoked by the WriteMultiTableTGroup command immediately after WriteMultiTable, this function groups multiple tables into the same T-Group. Each thread issues a write(..).get_precommit().await, so you isolate the cost of coordinating a multi-table commit. It lives directly below benchmark_write_tpcds_mt in src/main.rs.
