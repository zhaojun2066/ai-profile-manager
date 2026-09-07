//! Session persistence — session.json 读写，用于 agent 重启后恢复会话。
//!
//! 每个活跃会话在 `~/.kn/agent/sessions/{nid}/session.json` 中保存一份元数据。
//! agent 正常退出时会删除 session.json；异常崩溃时文件残留，重启时扫描恢复。

use crate::error::Result;
use crate::session::output::remove_replay_log;
use crate::session::types::{ManagedSession, SessionKind};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 持久化的会话元数据（与 ManagedSession 一一对应）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub nid: String,
    #[serde(rename = "kind")]
    pub kind: String, // "Native" | "Relay"
    pub source: String, // "ios" | "desktop"
    pub tool: String,
    pub profile: Option<String>,
    pub cwd: String,
    pub cols: u16,
    pub rows: u16,
    pub created_at: String, // ISO 8601
    pub pid: u32,
    pub remote_enabled: bool,
}

/// 每个 session 的持久化目录。
fn session_dir(nid: &str) -> PathBuf {
    kn_common::path::agent_dir().join("sessions").join(nid)
}

/// session.json 文件路径。
fn session_json_path(nid: &str) -> PathBuf {
    session_dir(nid).join("session.json")
}

// ── 写入 ──────────────────────────────────────────────────────

/// 将会话元数据写入 `session.json`。目录不存在则创建。
/// 使用原子写入：先写临时文件，再 rename。
pub fn write_session_record(session: &ManagedSession, pid: u32) -> Result<()> {
    let kind_str = match session.kind {
        SessionKind::Native => "Native",
        SessionKind::Relay => "Relay",
    };

    let record = SessionRecord {
        nid: session.nid.clone(),
        kind: kind_str.to_string(),
        source: session.source.clone(),
        tool: session.tool.clone(),
        profile: session.profile.clone(),
        cwd: session.cwd.clone(),
        cols: session.cols,
        rows: session.rows,
        created_at: session.created_at.to_rfc3339(),
        pid,
        remote_enabled: session
            .remote_enabled
            .load(std::sync::atomic::Ordering::Relaxed),
    };

    let dir = session_dir(&session.nid);
    std::fs::create_dir_all(&dir)?;

    let path = session_json_path(&session.nid);
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_string_pretty(&record)?;
    std::fs::write(&tmp, json)?;
    std::fs::rename(&tmp, &path)?;

    tracing::debug!(nid = %session.nid, pid = pid, "session.json 已写入");
    Ok(())
}

// ── 删除 ──────────────────────────────────────────────────────

/// 删除 session.json（会话正常结束时调用）。
pub fn delete_session_record(nid: &str) {
    let path = session_json_path(nid);
    if path.exists() {
        let _ = std::fs::remove_file(&path);
        tracing::debug!(nid = %nid, "session.json 已删除");
    }
    // 尝试清理空目录（非空时 remove_dir 会失败，由其他清理路径负责）
    let dir = session_dir(nid);
    if dir.exists() {
        let _ = std::fs::remove_dir(&dir);
    }
}

/// 更新已有的 session.json（remote_enabled 变化后调用）。
///
/// 防御性检查：如果会话已标记为 ended，直接删除文件而非写入，避免
/// 与 `report_session_ended` → `delete_session_record` 竞态导致的脏数据残留。
pub fn update_session_record(session: &ManagedSession, pid: u32) {
    // 会话已结束 → 删除而非写入（消除 TOCTOU 窗口）
    if session.ended_reported() {
        delete_session_record(&session.nid);
        return;
    }
    let path = session_json_path(&session.nid);
    if !path.exists() {
        return;
    }
    if let Err(e) = write_session_record(session, pid) {
        tracing::warn!(nid = %session.nid, error = %e, "更新 session.json 失败");
    }
}

// ── 扫描 ──────────────────────────────────────────────────────

/// 扫描所有残留的 session.json，返回记录列表。
/// 用于 agent 启动时恢复。
pub fn list_session_records() -> Result<Vec<SessionRecord>> {
    let sessions_dir = kn_common::path::agent_dir().join("sessions");
    if !sessions_dir.exists() {
        return Ok(Vec::new());
    }

    let mut records = Vec::new();
    let entries = std::fs::read_dir(&sessions_dir)?;

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        let json_path = entry.path().join("session.json");
        if !json_path.exists() {
            continue;
        }

        match std::fs::read_to_string(&json_path) {
            Ok(content) => match serde_json::from_str::<SessionRecord>(&content) {
                Ok(record) => records.push(record),
                Err(e) => {
                    tracing::warn!(path = %json_path.display(), error = %e, "session.json 解析失败，跳过");
                }
            },
            Err(e) => {
                tracing::warn!(path = %json_path.display(), error = %e, "session.json 读取失败，跳过");
            }
        }
    }

    Ok(records)
}

// ── 恢复 ──────────────────────────────────────────────────────

/// Agent 启动时扫描所有残留 session.json，恢复存活会话。
///
/// - 对每条记录调用 `kill(pid, 0)` 检查进程是否存活
/// - 存活 → 重建 ManagedSession 到 store，恢复 PID 映射
/// - 已死 → 删除 session.json + 目录
///
/// 返回成功恢复的会话数。
pub async fn recover_surviving_sessions(
    sessions: &crate::session::SessionManager,
) -> Result<usize> {
    let records = list_session_records()?;
    let mut recovered = 0usize;

    for record in &records {
        // kill(pid, 0) 检查进程存活
        #[cfg(unix)]
        let is_alive = unsafe { libc::kill(record.pid as i32, 0) == 0 };
        #[cfg(not(unix))]
        let is_alive = false;

        if !is_alive {
            tracing::info!(nid = %record.nid, pid = record.pid, "恢复扫描: 进程已死，清理");
            delete_session_record(&record.nid);
            remove_replay_log(&record.nid);
            continue;
        }

        // 重建 ManagedSession
        let kind = match record.kind.as_str() {
            "Relay" => SessionKind::Relay,
            _ => SessionKind::Native,
        };

        match sessions
            .create(
                record.nid.clone(),
                record.source.clone(),
                record.tool.clone(),
                record.profile.clone(),
                record.cwd.clone(),
                kind,
            )
            .await
        {
            Ok(session) => {
                // 恢复 PID
                sessions.set_child_pid(&record.nid, record.pid).await;
                // 恢复状态：直接信任 session.json 中的 remote_enabled
                session
                    .remote_enabled
                    .store(record.remote_enabled, std::sync::atomic::Ordering::Relaxed);
                // 更新 cols/rows
                let _ = sessions.resize(&record.nid, record.cols, record.rows).await;
                // 标记为 Running（进程活着）
                let _ = sessions.mark_running(&record.nid).await;

                tracing::info!(
                    nid = %record.nid, pid = record.pid, kind = %record.kind,
                    source = %record.source, tool = %record.tool,
                    remote = record.remote_enabled,
                    "🔄 会话已恢复"
                );
                recovered += 1;
            }
            Err(e) => {
                tracing::warn!(nid = %record.nid, error = %e, "恢复会话失败（可能已达上限），清理");
                delete_session_record(&record.nid);
                remove_replay_log(&record.nid);
            }
        }
    }

    if recovered > 0 {
        tracing::info!(
            recovered = recovered,
            total_scanned = records.len(),
            "会话恢复完成"
        );
    }

    Ok(recovered)
}

// ── Tests ──────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::types::SessionKind;
    use crate::session::SessionStatus;
    use chrono::Utc;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn make_test_session(nid: &str) -> ManagedSession {
        ManagedSession {
            kind: SessionKind::Native,
            nid: nid.to_string(),
            source: "ios".to_string(),
            tool: "claude".to_string(),
            profile: Some("test".to_string()),
            cwd: "/tmp".to_string(),
            cols: 80,
            rows: 24,
            viewport_owner: crate::session::types::ViewportOwner::Ios,
            created_at: Utc::now(),
            status: SessionStatus::Running,
            last_input: Arc::new(std::sync::Mutex::new(String::new())),
            last_output_snippet: Arc::new(std::sync::Mutex::new(String::new())),
            display_summary: Arc::new(std::sync::Mutex::new(None)),
            summary_input_buffer: Arc::new(std::sync::Mutex::new(String::new())),
            ended_reported: Arc::new(AtomicBool::new(false)),
            remote_enabled: Arc::new(AtomicBool::new(true)),
            relay_inputs: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    #[test]
    fn test_session_record_roundtrip_json() {
        let session = make_test_session("s_test123");
        let record = SessionRecord {
            nid: session.nid.clone(),
            kind: "Native".to_string(),
            source: session.source.clone(),
            tool: session.tool.clone(),
            profile: session.profile.clone(),
            cwd: session.cwd.clone(),
            cols: session.cols,
            rows: session.rows,
            created_at: session.created_at.to_rfc3339(),
            pid: 12345,
            remote_enabled: true,
        };

        let json = serde_json::to_string(&record).unwrap();
        let parsed: SessionRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.nid, "s_test123");
        assert_eq!(parsed.pid, 12345);
        assert!(parsed.remote_enabled);
    }

    #[test]
    fn test_list_empty_when_no_sessions() {
        // Use a non-existent directory by overriding the path?
        // Actually we can't easily mock kn_common::path::agent_dir().
        // Just verify the function doesn't panic.
        let records = list_session_records();
        assert!(records.is_ok());
    }

    #[test]
    fn test_delete_nonexistent_is_noop() {
        // Should not panic
        delete_session_record("s_nonexistent_12345");
    }
}
