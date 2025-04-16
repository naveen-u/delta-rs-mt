#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // let table_path = "test/tests/data/delta-0.8.0";
    // let table_path = "test/tests/data/simple_table_with_checkpoint";
    let table_path = "test/tests/data/t_group_table_2";
    // let table_path = "test/tests/data/_tgroup_delta_log";
    let table = deltalake::open_table(table_path).await?;
    println!("mewmew");
    println!("{table}");
    Ok(())
}
