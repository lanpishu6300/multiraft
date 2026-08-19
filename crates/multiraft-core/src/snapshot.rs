//! Standby async snapshot trigger + advertisement types (Aeron-aligned).

use serde::Deserialize;
use serde::Serialize;

/// Magic payload: Leader proposes this; Standby applies it and schedules an async snapshot.
pub const STANDBY_SNAPSHOT_TRIGGER: &[u8] = b"\0multiraft.standby_snapshot_trigger\0";

/// Returns true when `data` is the standby snapshot trigger magic bytes.
pub fn is_standby_snapshot_trigger(data: &[u8]) -> bool {
    data == STANDBY_SNAPSHOT_TRIGGER
}

/// Advertisement published by a Standby after a durable snapshot is written.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SnapshotAdvertisement {
    pub group: u64,
    pub last_index: u64,
    pub last_term: u64,
    pub snapshot_id: String,
    pub size: u64,
    pub sha256_hex: String,
    /// e.g. `http://127.0.0.1:23103/snapshots/0/latest`
    pub fetch_url: String,
}

/// Result of auto-recovery from standby snapshot advertisements.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum RecoverOutcome {
    Installed { last_index: u64, last_term: u64 },
    SkippedNoAd,
    SkippedNotNewer { local_index: u64, ad_index: u64 },
    FetchFailed { error: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recover_outcome_serde_roundtrip() {
        let cases = [
            RecoverOutcome::Installed {
                last_index: 9,
                last_term: 2,
            },
            RecoverOutcome::SkippedNoAd,
            RecoverOutcome::SkippedNotNewer {
                local_index: 3,
                ad_index: 3,
            },
            RecoverOutcome::FetchFailed {
                error: "timeout".into(),
            },
        ];
        for c in cases {
            let s = serde_json::to_string(&c).unwrap();
            let back: RecoverOutcome = serde_json::from_str(&s).unwrap();
            assert_eq!(back, c);
        }
        let installed = serde_json::to_value(RecoverOutcome::Installed {
            last_index: 1,
            last_term: 1,
        })
        .unwrap();
        assert_eq!(installed["outcome"], "installed");
    }

    #[test]
    fn standby_trigger_and_advertisement() {
        assert!(is_standby_snapshot_trigger(STANDBY_SNAPSHOT_TRIGGER));
        assert!(!is_standby_snapshot_trigger(b"other"));
        let ad = SnapshotAdvertisement {
            group: 1,
            last_index: 10,
            last_term: 2,
            snapshot_id: "s1".into(),
            size: 64,
            sha256_hex: "abc".into(),
            fetch_url: "http://127.0.0.1/s".into(),
        };
        let s = serde_json::to_string(&ad).unwrap();
        let back: SnapshotAdvertisement = serde_json::from_str(&s).unwrap();
        assert_eq!(back, ad);
        let _ = format!("{ad:?}");
    }
}
