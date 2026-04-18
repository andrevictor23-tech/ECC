pub mod daemon;
pub mod manager;
pub mod output;
pub mod runtime;
pub mod store;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub task: String,
    pub agent_type: String,
    pub working_dir: PathBuf,
    pub state: SessionState,
    pub pid: Option<u32>,
    pub worktree: Option<WorktreeInfo>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub metrics: SessionMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SessionState {
    Pending,
    Running,
    Idle,
    Completed,
    Failed,
    Stopped,
}

impl fmt::Display for SessionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionState::Pending => write!(f, "pending"),
            SessionState::Running => write!(f, "running"),
            SessionState::Idle => write!(f, "idle"),
            SessionState::Completed => write!(f, "completed"),
            SessionState::Failed => write!(f, "failed"),
            SessionState::Stopped => write!(f, "stopped"),
        }
    }
}

impl SessionState {
    pub fn can_transition_to(&self, next: &Self) -> bool {
        if self == next {
            return true;
        }

        matches!(
            (self, next),
            (
                SessionState::Pending,
                SessionState::Running | SessionState::Failed | SessionState::Stopped
            ) | (
                SessionState::Running,
                SessionState::Idle
                    | SessionState::Completed
                    | SessionState::Failed
                    | SessionState::Stopped
            ) | (
                SessionState::Idle,
                SessionState::Running
                    | SessionState::Completed
                    | SessionState::Failed
                    | SessionState::Stopped
            ) | (SessionState::Completed, SessionState::Stopped)
                | (SessionState::Failed, SessionState::Stopped)
        )
    }

    pub fn from_db_value(value: &str) -> Self {
        match value {
            "running" => SessionState::Running,
            "idle" => SessionState::Idle,
            "completed" => SessionState::Completed,
            "failed" => SessionState::Failed,
            "stopped" => SessionState::Stopped,
            _ => SessionState::Pending,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorktreeInfo {
    pub path: PathBuf,
    pub branch: String,
    pub base_branch: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionMetrics {
    pub tokens_used: u64,
    pub tool_calls: u64,
    pub files_changed: u32,
    pub duration_secs: u64,
    pub cost_usd: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionBoardMeta {
    pub lane: String,
    pub project: Option<String>,
    pub feature: Option<String>,
    pub issue: Option<String>,
    pub row_label: Option<String>,
    pub previous_lane: Option<String>,
    pub previous_row_label: Option<String>,
    pub column_index: i64,
    pub row_index: i64,
    pub stack_index: i64,
    pub progress_percent: i64,
    pub status_detail: Option<String>,
    pub movement_note: Option<String>,
    pub activity_kind: Option<String>,
    pub activity_note: Option<String>,
    pub handoff_backlog: i64,
    pub conflict_signal: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionMessage {
    pub id: i64,
    pub from_session: String,
    pub to_session: String,
    pub content: String,
    pub msg_type: String,
    pub read: bool,
    pub timestamp: DateTime<Utc>,
}
