use std::sync::Arc;
use deltalake::arrow::{
    array::Int32Array,
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use deltalake::DeltaOps;

use deltalake::kernel::{Action, CommitInfo, EagerSnapshot, Metadata, Protocol, Transaction};
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // Open the existing Delta table with checkpoint.
    let table_path = "test/tests/data/simple_table_with_checkpoint";
    let table: deltalake::DeltaTable = deltalake::open_table(table_path).await?;
    println!("{table}");
    
    println!("Opened Delta table at: {}", table_path);
    println!("Table schema: {:?}", table.schema());
    
    // Build an Arrow schema matching the Delta table's schema.
    // In our case, the table has a single field "version" of type Int32.
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("version", ArrowDataType::Int32, true)
    ]));
    
    // Create a record batch with a hardcoded tuple (here, version = 42).
    let version_array = Int32Array::from(vec![42]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(version_array)])?;
    
    // Write the new record batch to the existing Delta table.
    // let table = DeltaOps(table)
    //     .write(vec![batch])
    //     .await?;

    let actions = DeltaOps(table)
        .write_tgroup(vec![batch])
        .get_precommit_actions().await?;

    // Display the commit actions.
    println!("Precommit actions: {:#?}", actions);



    //write_tgroup().return_actions();
    
    // println!("Delta table written successfully. Current version: {}", table.version());
    Ok(())
}