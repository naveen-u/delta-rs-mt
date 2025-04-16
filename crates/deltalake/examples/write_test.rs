use deltalake::arrow::{
    array::Int32Array,
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use deltalake::kernel::Action;
use deltalake::DeltaOps;
use std::sync::Arc;

use deltalake_core::errors::DeltaResult;
use deltalake_core::operations::transaction::PreCommit;

fn update_action_with_table_id(action: &Action, table_uuid: &str) -> Action {
    match action {
        Action::Metadata(meta) => {
            let mut new_meta = meta.clone();
            new_meta.table_id = Some(table_uuid.to_owned());
            Action::Metadata(new_meta)
        }
        Action::Txn(txn) => {
            let mut new_txn = txn.clone();
            new_txn.table_id = Some(table_uuid.to_owned());
            Action::Txn(new_txn)
        }
        Action::CommitInfo(ci) => {
            let mut new_ci = ci.clone();
            new_ci.table_id = Some(table_uuid.to_owned());
            Action::CommitInfo(new_ci)
        }
        Action::Remove(rem) => {
            let mut new_rem = rem.clone();
            new_rem.table_id = Some(table_uuid.to_owned());
            Action::Remove(new_rem)
        }
        Action::Add(add) => {
            let mut new_add = add.clone();
            new_add.table_id = Some(table_uuid.to_owned());
            Action::Add(new_add)
        }
        Action::Protocol(proto) => {
            let mut new_proto = proto.clone();
            new_proto.table_id = Some(table_uuid.to_owned());
            Action::Protocol(new_proto)
        }
        // For all other Action variants that do not have a table_id field,
        // simply clone the action.
        _ => action.clone(),
    }
}

fn combine_precommits_with_table_id<'a>(
    precommits: Vec<PreCommit<'a>>,
    tables: Vec<String>,
) -> DeltaResult<PreCommit<'a>> {
    let mut combined_actions: Vec<Action> = Vec::new();

    // Process every provided precommit.
    for (precommit, table_uuid) in precommits.iter().zip(tables.iter()) {
        for action in precommit.data().actions.iter() {
            combined_actions.push(update_action_with_table_id(action, &table_uuid));
        }
    }

    let mut combined_precommit = precommits
        .into_iter()
        .next()
        .expect("No precommits provided");
    combined_precommit.data_mut().actions = combined_actions;
    Ok(combined_precommit)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // Open the existing Delta table with checkpoint.
    let table_path = "test/tests/data/simple_table_with_checkpoint";
    let table1: deltalake::DeltaTable = deltalake::open_table(table_path).await?;
    println!("{table1}");

    println!("Opened Delta table at: {}", table_path);
    println!("Table schema: {:?}", table1.schema());

    // let table_path = "/Users/anirudhbhaskar/delta-rs-mt/crates/test/tests/data/simple_table";
    // let table1: deltalake::DeltaTable = deltalake::open_table(table_path).await?;

    // Build an Arrow schema matching the Delta table's schema.
    // In our case, the table has a single field "version" of type Int32.
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "version",
        ArrowDataType::Int32,
        true,
    )]));

    // Create a record batch with a hardcoded tuple (here, version = 42).
    let version_array = Int32Array::from(vec![42]);
    let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![42]))])?;

    let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![43]))])?;

    let batch3 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![44]))])?;

    // table uuid
    // table1.metadata().unwrap().id

    // Write the new record batch to the existing Delta table.
    // let table = DeltaOps(table)
    //     .write(vec![batch])
    //     .await?;

    let uuid1 = table1.metadata().unwrap().id.clone();
    let uuid2 = table1.metadata().unwrap().id.clone();
    let uuid3 = table1.metadata().unwrap().id.clone();

    let precommit1 = DeltaOps(table1.clone())
        .write_tgroup(vec![batch1])
        .get_precommit()
        .await?;

    let precommit2 = DeltaOps(table1.clone())
        .write_tgroup(vec![batch2])
        .get_precommit()
        .await?;

    let precommit3 = DeltaOps(table1.clone())
        .write_tgroup(vec![batch3])
        .get_precommit()
        .await?;

    let combined_precommit = combine_precommits_with_table_id(
        vec![precommit1, precommit2, precommit3],
        vec![uuid1, uuid2, uuid3],
    )?;

    println!(
        "Combined Precommit Actions: {:#?}",
        combined_precommit.data().actions
    );
    let final_commit = combined_precommit.await?;
    println!("Final commit version: {}", final_commit.snapshot.version());

    // Display the commit actions.
    //println!("Precommit actions: {:#?}", precommit.data().actions);

    // need to find a way to get uuid
    // let table_uuid: String = "abc";

    //write_tgroup().return_actions();

    // println!("Delta table written successfully. Current version: {}", table.version());
    Ok(())
}
