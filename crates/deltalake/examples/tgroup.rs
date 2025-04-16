use deltalake::arrow::{
    array::Int32Array,
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use deltalake::DeltaOps;
use std::sync::Arc;

use deltalake::kernel::{Action, CommitInfo, EagerSnapshot, Metadata, Protocol, Transaction};
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // Open the existing Delta table with checkpoint.
    let table_path1 = "test/tests/data/simple_table";
    let table_path2 = "test/tests/data/simple_table_with_checkpoint";
    let mut table1: deltalake::DeltaTable = deltalake::open_table(table_path1).await?;
    let mut table2: deltalake::DeltaTable = deltalake::open_table(table_path2).await?;
    let tgroup_path = "test/tests/data/tgroup_1";
    table1.add_to_tgroup(tgroup_path);
    table2.add_to_tgroup(tgroup_path);
    Ok(())
}
