use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::KronError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Schedule {
    /// Standard 5-field cron expression: `min hour day month weekday`
    Cron { expr: String },
    /// Repeat every fixed duration, e.g. "30m", "1h"
    Every { seconds: u64 },
    /// Fire once at an absolute UTC time
    At { at: DateTime<Utc> },
    /// Fire once after a delay from registration
    After {
        seconds: u64,
        registered_at: DateTime<Utc>,
    },
}

impl Schedule {
    /// Compute the next run time after `after`, in the given IANA timezone.
    /// Returns `None` for one-shot schedules that have already fired.
    pub fn next_run_after(
        &self,
        after: DateTime<Utc>,
        tz: &str,
    ) -> Result<Option<DateTime<Utc>>, KronError> {
        let timezone: Tz = tz
            .parse()
            .map_err(|_| KronError::InvalidTimezone(tz.to_string()))?;

        match self {
            Schedule::Cron { expr } => {
                // The `cron` crate expects 7 fields (sec min hour dom mon dow year).
                // We accept standard 5-field cron and prepend "0" (seconds) and
                // append "*" (year) automatically.
                let full_expr = format!("0 {} *", expr);
                let sched = cron::Schedule::from_str(&full_expr)
                    .map_err(|e| KronError::InvalidCron(e.to_string()))?;

                // Compute next occurrence in the local timezone, convert back to UTC.
                let after_local = after.with_timezone(&timezone);
                let next = sched.after(&after_local).next();
                Ok(next.map(|t| t.with_timezone(&Utc)))
            }

            Schedule::Every { seconds } => {
                let delta = Duration::seconds(*seconds as i64);
                Ok(Some(after + delta))
            }

            Schedule::At { at } => {
                if *at > after {
                    Ok(Some(*at))
                } else {
                    Ok(None) // already fired
                }
            }

            Schedule::After {
                seconds,
                registered_at,
            } => {
                let fire_at = *registered_at + Duration::seconds(*seconds as i64);
                if fire_at > after {
                    Ok(Some(fire_at))
                } else {
                    Ok(None) // already fired
                }
            }
        }
    }

    /// Convenience: is this a one-shot schedule?
    pub fn is_one_shot(&self) -> bool {
        matches!(self, Schedule::At { .. } | Schedule::After { .. })
    }
}

// ---------------------------------------------------------------------------
// Helpers for building Schedule from user-facing strings
// ---------------------------------------------------------------------------

/// Parse "30m", "1h", "90s" into seconds.
pub fn parse_duration_str(s: &str) -> Result<u64, KronError> {
    let s = s.trim();
    if let Some(stripped) = s.strip_suffix('s') {
        stripped
            .parse::<u64>()
            .map_err(|_| KronError::InvalidCron(format!("invalid duration: {}", s)))
    } else if let Some(stripped) = s.strip_suffix('m') {
        stripped
            .parse::<u64>()
            .map(|n| n * 60)
            .map_err(|_| KronError::InvalidCron(format!("invalid duration: {}", s)))
    } else if let Some(stripped) = s.strip_suffix('h') {
        stripped
            .parse::<u64>()
            .map(|n| n * 3600)
            .map_err(|_| KronError::InvalidCron(format!("invalid duration: {}", s)))
    } else if let Some(stripped) = s.strip_suffix('d') {
        stripped
            .parse::<u64>()
            .map(|n| n * 86400)
            .map_err(|_| KronError::InvalidCron(format!("invalid duration: {}", s)))
    } else {
        Err(KronError::InvalidCron(format!(
            "duration must end with s/m/h/d: {}",
            s
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;

    #[test]
    fn parses_duration_suffixes() {
        assert_eq!(parse_duration_str("90s").unwrap(), 90);
        assert_eq!(parse_duration_str("30m").unwrap(), 1800);
        assert_eq!(parse_duration_str("2h").unwrap(), 7200);
        assert_eq!(parse_duration_str("1d").unwrap(), 86400);
        assert!(parse_duration_str("10").is_err());
    }

    #[test]
    fn cron_uses_requested_timezone() {
        let schedule = Schedule::Cron {
            expr: "0 8 * * *".to_string(),
        };
        let after = Utc.with_ymd_and_hms(2026, 6, 10, 5, 30, 0).unwrap();
        let next = schedule
            .next_run_after(after, "Europe/Rome")
            .unwrap()
            .unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 6, 10, 6, 0, 0).unwrap());
    }

    #[test]
    fn cron_handles_europe_rome_spring_forward() {
        let schedule = Schedule::Cron {
            expr: "30 2 * * *".to_string(),
        };
        let after = Utc.with_ymd_and_hms(2026, 3, 28, 23, 0, 0).unwrap();
        let next = schedule
            .next_run_after(after, "Europe/Rome")
            .unwrap()
            .unwrap();
        assert!(next > after);
    }

    #[test]
    fn cron_handles_europe_rome_fall_back() {
        let schedule = Schedule::Cron {
            expr: "30 2 * * *".to_string(),
        };
        let after = Utc.with_ymd_and_hms(2026, 10, 24, 23, 0, 0).unwrap();
        let next = schedule
            .next_run_after(after, "Europe/Rome")
            .unwrap()
            .unwrap();
        assert!(next > after);
    }

    #[test]
    fn invalid_timezone_is_rejected() {
        let schedule = Schedule::Every { seconds: 10 };
        assert!(schedule.next_run_after(Utc::now(), "Nope/Nowhere").is_err());
    }

    #[test]
    fn one_shot_schedule_returns_none_after_fire_time() {
        let at = Utc.with_ymd_and_hms(2026, 6, 10, 8, 0, 0).unwrap();
        let schedule = Schedule::At { at };
        assert!(schedule
            .next_run_after(at + Duration::seconds(1), "UTC")
            .unwrap()
            .is_none());
    }

    proptest! {
        #[test]
        fn parses_seconds_minutes_hours_days(n in 1u64..10_000, suffix in "[smhd]") {
            let input = format!("{n}{suffix}");
            let parsed = parse_duration_str(&input).unwrap();
            prop_assert!(parsed >= n);
        }
    }
}
