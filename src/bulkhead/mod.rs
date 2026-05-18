//! Bulkhead — limits concurrent in-flight operations.
//!
//! The bulkhead pattern isolates failures and resource contention by capping
//! how many calls may run at the same time. When the limit is reached, new
//! calls are rejected immediately instead of piling up unbounded work.
//!
//! # Example
//!
//! ```ignore
//! use resilient::bulkhead::Bulkhead;
//!
//! let bulkhead = Bulkhead::default().with_max_concurrent(8);
//!
//! let result = bulkhead
//!     .run(|| async { Ok::<_, String>("done") })
//!     .await;
//! ```

pub mod bulkhead_policy;
pub mod errors;

pub use bulkhead_policy::Bulkhead;
pub use errors::BulkheadError;
