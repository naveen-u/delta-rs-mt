use std::sync::Arc;
use deltalake::{DeltaOps, DeltaTable, DeltaResult};
use deltalake::arrow::{
    array::Int32Array,
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use deltalake_core::operations::transaction::PreCommit;
use deltalake_core::kernel::Action;
use deltalake_core::errors::DeltaTableError;

fn update_action_with_tableuuid(action: &Action, table_uuid: &str) -> Action {
    match action {
        Action::Metadata(meta) => {
            let mut new_meta = meta.clone();
            new_meta.tableuuid = Some(table_uuid.to_owned());
            Action::Metadata(new_meta)
        }
        Action::Txn(txn) => {
            let mut new_txn = txn.clone();
            new_txn.tableuuid = Some(table_uuid.to_owned());
            Action::Txn(new_txn)
        }
        Action::CommitInfo(ci) => {
            let mut new_ci = ci.clone();
            new_ci.tableuuid = Some(table_uuid.to_owned());
            Action::CommitInfo(new_ci)
        }
        Action::Remove(rem) => {
            let mut new_rem = rem.clone();
            new_rem.tableuuid = Some(table_uuid.to_owned());
            Action::Remove(new_rem)
        }
        Action::Add(add) => {
            let mut new_add = add.clone();
            new_add.tableuuid = Some(table_uuid.to_owned());
            Action::Add(new_add)
        }
        Action::Protocol(proto) => {
            let mut new_proto = proto.clone();
            new_proto.tableuuid = Some(table_uuid.to_owned());
            Action::Protocol(new_proto)
        }
        // For all other Action variants that do not have a tableuuid field,
        // simply clone the action.
        _ => action.clone(),
    }
}


/// Combine a vector of precommits with their corresponding table UUIDs.
/// For each pair, update every action with the provided UUID, then
/// use the first precommit as baseline and override its actions with the combined list.
fn combine_precommits_with_tableuuid<'a>(
    precommits: Vec<PreCommit<'a>>,
    table_uuids: Vec<String>,
) -> DeltaResult<PreCommit<'a>> {
    let mut combined_actions: Vec<Action> = Vec::new();

    // Iterate over the precommits and corresponding UUIDs.
    for (precommit, table_uuid) in precommits.iter().zip(table_uuids.iter()) {
        for action in precommit.data().actions.iter() {
            combined_actions.push(update_action_with_tableuuid(action, table_uuid));
        }
    }

    // Use the first precommit as a baseline and replace its actions.
    let mut combined_precommit = precommits.into_iter().next()
        .expect("No precommits provided");
    combined_precommit.data_mut().actions = combined_actions;
    Ok(combined_precommit)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> DeltaResult<()> {
    // Open two different Delta tables.
    let table_path1 = "test/tests/data/simple_table";
    let mut table1: DeltaTable = deltalake::open_table(table_path1).await?;
    
    let table_path2 = "test/tests/data/simple_table_with_checkpoint";
    let mut table2: DeltaTable = deltalake::open_table(table_path2).await?;
    
    println!("Opened Table 1 from: {}", table_path1);
    println!("Opened Table 2 from: {}", table_path2);
    
    // Build an Arrow schema matching the table's schema.
    // (For this example, the table has a single field "version" of type Int32.)

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("version", ArrowDataType::Int32, true),
    ]));
    
    // Create a record batch for each table with different data.
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )?;
    
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![2]))],
    )?;
    
    // Extract the table UUIDs before moving the tables.
    let uuid1 = table1.metadata().unwrap().id.clone();
    let uuid2 = table2.metadata().unwrap().id.clone();

    println!("UUID1 {}",uuid1);
    println!("UUID2 {}",uuid2);
    
    
    // Create one precommit per table (the table is moved here).
    let precommit1 = DeltaOps(table1)
        .write_tgroup(vec![batch1])
        .get_precommit().await?;
        
    let precommit2 = DeltaOps(table2)
        .write_tgroup(vec![batch2])
        .get_precommit().await?;
    
    // Combine the two precommits using the respective UUIDs.
    let combined_precommit = combine_precommits_with_tableuuid(
        vec![precommit1, precommit2],
        vec![uuid1, uuid2],
    )?;
    
    println!("Combined Precommit Actions: {:#?}", combined_precommit.data().actions);
    
    // Finalize the commit using the combined precommit.
    let final_commit = combined_precommit.await?;
    println!("Final commit version: {}", final_commit.snapshot.version());
    
    Ok(())
}
