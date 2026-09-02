//! Forge configuration materialization for local image overrides.

use std::{
    fs,
    path::{Path, PathBuf},
};

use super::image_overrides;

/// Render a Forge environment with the explicitly selected demo images.
pub(crate) fn materialize(source: &Path, output: Option<&Path>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(source)?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(&content)?;
    apply_image_overrides(&mut config)?;
    let destination = output.map_or_else(
        || {
            source.with_file_name(format!(
                "{}.resolved.yaml",
                source.file_stem().and_then(|s| s.to_str()).unwrap_or("forge")
            ))
        },
        Path::to_path_buf,
    );
    fs::write(&destination, serde_yaml::to_string(&config)?)?;
    Ok(destination)
}

#[expect(
    clippy::too_many_lines,
    reason = "The bounded image-property rewrite is easiest to audit as one operation."
)]
/// Apply the selected image references to every Forge cluster property.
fn apply_image_overrides(config: &mut serde_yaml::Value) -> Result<(), Box<dyn std::error::Error>> {
    let pull_policy = image_overrides::image_pull_policy();
    let gateway = image_overrides::gateway_image();
    let operator = image_overrides::operator_image();
    let overlay_sync = image_overrides::overlay_sync_image();
    let vcr = image_overrides::vcr_image();

    if pull_policy == "Never"
        && (std::env::var_os("GRID_XTASK_GATEWAY_IMAGE").is_none()
            || std::env::var_os("GRID_XTASK_OPERATOR_IMAGE").is_none()
            || std::env::var_os("GRID_XTASK_OVERLAY_SYNC_IMAGE").is_none())
    {
        return Err("GRID_XTASK_GATEWAY_IMAGE, GRID_XTASK_OPERATOR_IMAGE, and GRID_XTASK_OVERLAY_SYNC_IMAGE are required when GRID_XTASK_IMAGE_PULL_POLICY=Never".into());
    }

    let (gateway_repo, gateway_tag) = parse_image_ref(&gateway);
    let (operator_repo, operator_tag) = parse_image_ref(&operator);
    let (overlay_repo, overlay_tag) = parse_image_ref(&overlay_sync);

    let clusters = config
        .get_mut("spec")
        .and_then(|spec| spec.get_mut("clusters"))
        .and_then(serde_yaml::Value::as_sequence_mut)
        .ok_or("Forge config must contain spec.clusters")?;

    for cluster in clusters {
        let properties = cluster
            .get_mut("properties")
            .and_then(serde_yaml::Value::as_mapping_mut)
            .ok_or("Forge cluster must contain properties")?;
        for (key, value) in [
            ("gatewayImage", gateway.clone()),
            ("gatewayImageRepo", gateway_repo.clone()),
            ("gatewayImageTag", gateway_tag.clone()),
            ("operatorImage", operator.clone()),
            ("operatorImageRepo", operator_repo.clone()),
            ("operatorImageTag", operator_tag.clone()),
            ("overlaySyncImage", overlay_sync.clone()),
            ("overlaySyncImageRepo", overlay_repo.clone()),
            ("overlaySyncImageTag", overlay_tag.clone()),
            ("vcrImage", vcr.clone()),
            ("imagePullPolicy", pull_policy.clone()),
        ] {
            properties.insert(
                serde_yaml::Value::String(key.to_owned()),
                serde_yaml::Value::String(value),
            );
        }
    }
    Ok(())
}

/// Split an image reference into repository and tag components.
fn parse_image_ref(image: &str) -> (String, String) {
    let last_slash = image.rfind('/');
    image
        .rfind(':')
        .filter(|colon| last_slash.is_none_or(|slash| *colon > slash))
        .map_or_else(
            || (image.to_owned(), "latest".to_owned()),
            |colon| {
                let (repo, tagged) = image.split_at(colon);
                (repo.to_owned(), tagged.strip_prefix(':').unwrap_or_default().to_owned())
            },
        )
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::parse_image_ref;

    #[test]
    fn parses_tagged_and_untagged_images() {
        assert_eq!(
            parse_image_ref("repo/image:tag"),
            ("repo/image".to_owned(), "tag".to_owned())
        );
        assert_eq!(
            parse_image_ref("repo/image"),
            ("repo/image".to_owned(), "latest".to_owned())
        );
        assert_eq!(
            parse_image_ref("localhost:5000/image"),
            ("localhost:5000/image".to_owned(), "latest".to_owned())
        );
    }

    #[test]
    #[expect(
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::too_many_lines,
        reason = "A test fixture should fail at the exact missing quota-contract field."
    )]
    fn quota_consumers_use_the_upstream_limiter_schema_and_shared_rule() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tests/e2e/topologies/grid-token-rate-limit");
        let configs = [
            root.join("configs/consumer/praxis-valkey-a.yaml"),
            root.join("configs/consumer/praxis-valkey-b.yaml"),
        ];
        let mut quota_contract = None;

        for path in configs {
            let source = fs::read_to_string(&path).expect("read quota consumer config");
            for removed in ["reservationTimeout", "token_budgets", "estimation", "identity.user_id"] {
                assert!(
                    !source.contains(removed),
                    "{} still contains legacy field {removed}",
                    path.display()
                );
            }
            assert!(
                !source.contains("username: bob"),
                "{} must remain a single-principal qualification",
                path.display()
            );

            let config: serde_yaml::Value = serde_yaml::from_str(&source).expect("parse quota consumer config");
            let filters = config["filter_chains"][0]["filters"]
                .as_sequence()
                .expect("filter chain must contain filters");
            let limiter = filters
                .iter()
                .find(|filter| filter["filter"].as_str() == Some("token_rate_limit"))
                .expect("token_rate_limit filter must exist");
            let contract = (
                limiter["backend"]["namespace"].as_str().expect("namespace").to_owned(),
                limiter["rules"][0]["name"].as_str().expect("rule name").to_owned(),
                limiter["rules"][0]["algorithm"].as_str().expect("algorithm").to_owned(),
                limiter["rules"][0]["window"].as_str().expect("window").to_owned(),
                limiter["rules"][0]["capacity"].as_u64().expect("capacity"),
                limiter["rules"][0]["reserved_tokens"]
                    .as_u64()
                    .expect("reserved tokens"),
                limiter["rules"][0]["reservation_timeout"]
                    .as_str()
                    .expect("reservation timeout")
                    .to_owned(),
            );
            assert_eq!(
                contract,
                (
                    "praxis:grid-token-rate-limit".to_owned(),
                    "alice-shared-budget".to_owned(),
                    "sliding_window".to_owned(),
                    "60s".to_owned(),
                    60,
                    15,
                    "30s".to_owned(),
                )
            );
            assert_eq!(
                limiter["rules"][0]["match"]["headers"]["x-model"].as_str(),
                Some("Qwen/Qwen3-0.6B"),
                "quota must apply only to the validated inference model"
            );

            if let Some(expected) = quota_contract.as_ref() {
                assert_eq!(&contract, expected, "both consumers must address the same Valkey rule");
            } else {
                quota_contract = Some(contract);
            }
        }
    }
}
