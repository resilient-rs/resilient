use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BulkheadError {
    #[error("Bulkhead capacity exceeded: max concurrent calls in flight")]
    CapacityExceeded,
}

impl From<BulkheadError> for String {
    fn from(e: BulkheadError) -> Self {
        e.to_string()
    }
}
