use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum BulkheadError {
    #[error("Bulkhead capacity exceeded: max concurrent calls in flight")]
    CapacityExceeded,
}
