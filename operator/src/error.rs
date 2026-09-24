//! Operator error types.

// ---------------------------------------------------------------------------
// Operator Error
// ---------------------------------------------------------------------------

/// Errors produced by the AGN Operator (`grid-operator`).
#[derive(Debug, thiserror::Error)]
pub enum OperatorError {
    /// Certificate generation failed.
    #[error("certificate error: {0}")]
    Certificate(#[from] certs::GenerateError),

    /// Kubernetes API error.
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    /// JSON serialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A required resource was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Routing overlay rendering failed.
    #[error("overlay render: {0}")]
    OverlayRender(String),

    /// Consumer Praxis config rendering failed.
    #[error("consumer config render: {0}")]
    ConsumerConfigRender(#[from] crate::resources::consumer_config::ConsumerConfigError),

    /// SWIM encryption key configuration failed.
    #[error("swim key configuration: {0}")]
    SwimKeyConfig(String),

    /// A watched resource is missing required metadata.
    #[error("invalid resource: {0}")]
    InvalidResource(String),
}
