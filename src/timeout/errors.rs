use std::time::Duration;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TimeoutError {
    #[error(
        "Operation timed out after {duration:?}{}",
        match .name {
            Some(n) => format!(" ({n})"),
            None => String::new(),
        }
    )]
    Elapsed {
        duration: Duration,
        name: Option<String>,
    },

    #[error(transparent)]
    Returning(Box<dyn std::error::Error + Send + Sync>),
}
