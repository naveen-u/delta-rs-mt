//! Actions are the fundamental unit of work in Delta Lake. Each action performs a single atomic
//! operation on the state of a Delta table. Actions are stored in the `_delta_log` directory of a
//! Delta table in JSON format. The log is a time series of actions that represent all the changes
//! made to a table.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub(crate) mod actions;
pub(crate) mod fields;
mod schema;

pub use actions::*;
pub use schema::*;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
/// The type of action that was performed on the table
pub enum ActionType {
    /// modify the data in a table by adding individual logical files
    Add,
    /// add a file containing only the data that was changed as part of the transaction
    Cdc,
    /// additional provenance information about what higher-level operation was being performed
    CommitInfo,
    /// contains a configuration (string-string map) for a named metadata domain
    DomainMetadata,
    /// changes the current metadata of the table
    Metadata,
    /// increase the version of the Delta protocol that is required to read or write a given table
    Protocol,
    /// modify the data in a table by removing individual logical files
    Remove,
    /// Transactional information
    Txn,
    /// Checkpoint metadata
    CheckpointMetadata,
    /// TGroup redirection,
    TGroup,
    /// Sidecar
    Sidecar,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(missing_docs)]
pub enum Action {
    #[serde(rename = "metaData")]
    Metadata(Metadata),
    Protocol(Protocol),
    Add(Add),
    Remove(Remove),
    Cdc(AddCDCFile),
    Txn(Transaction),
    CommitInfo(CommitInfo),
    DomainMetadata(DomainMetadata),
    TGroup(TGroup),
}

impl Action {
    /// Create a commit info from a map
    pub fn commit_info(info: HashMap<String, serde_json::Value>) -> Self {
        Self::CommitInfo(CommitInfo {
            info,
            ..Default::default()
        })
    }
}

impl From<Add> for Action {
    fn from(a: Add) -> Self {
        Self::Add(a)
    }
}

impl From<Remove> for Action {
    fn from(a: Remove) -> Self {
        Self::Remove(a)
    }
}

impl From<AddCDCFile> for Action {
    fn from(a: AddCDCFile) -> Self {
        Self::Cdc(a)
    }
}

impl From<Metadata> for Action {
    fn from(a: Metadata) -> Self {
        Self::Metadata(a)
    }
}

impl From<Protocol> for Action {
    fn from(a: Protocol) -> Self {
        Self::Protocol(a)
    }
}

impl From<Transaction> for Action {
    fn from(a: Transaction) -> Self {
        Self::Txn(a)
    }
}

impl From<CommitInfo> for Action {
    fn from(a: CommitInfo) -> Self {
        Self::CommitInfo(a)
    }
}

impl From<DomainMetadata> for Action {
    fn from(a: DomainMetadata) -> Self {
        Self::DomainMetadata(a)
    }
}

impl From<TGroup> for Action {
    fn from(a: TGroup) -> Self {
        Self::TGroup(a)
    }
}

impl Action {
    /// Get the action type
    pub fn action_type(&self) -> ActionType {
        match self {
            Self::Add(_) => ActionType::Add,
            Self::Remove(_) => ActionType::Remove,
            Self::Cdc(_) => ActionType::Cdc,
            Self::Metadata(_) => ActionType::Metadata,
            Self::Protocol(_) => ActionType::Protocol,
            Self::Txn(_) => ActionType::Txn,
            Self::CommitInfo(_) => ActionType::CommitInfo,
            Self::DomainMetadata(_) => ActionType::DomainMetadata,
            Self::TGroup(_) => ActionType::TGroup,
        }
    }

    pub fn update_action_with_table_id(&mut self, table_uuid: String) -> &mut Self {
        match self {
            Action::Metadata(meta) => meta.table_id = Some(table_uuid),
            Action::Txn(txn) => txn.table_id = Some(table_uuid),
            Action::CommitInfo(ci) => ci.table_id = Some(table_uuid),
            Action::Remove(rem) => rem.table_id = Some(table_uuid),
            Action::Add(add) => add.table_id = Some(table_uuid),
            Action::Protocol(proto) => proto.table_id = Some(table_uuid),
            // For all other Action variants that do not have a table_id field,
            // simply clone the action.
            _ => {}
        };
        self
    }
}
