use super::{pty_sock_path, PtyAttachHandle};
use crate::error::{AgentError, Result};
use crate::session::env::{prepare_tool_env, resolve_tool_path};
use crate::session::input::InputMerger;
use crate::session::output::{remove_replay_log, OutputFanout};
use crate::session::store::SessionStore;
use crate::session::types::{
    ManagedSession, SessionKind, SessionStatus, SessionSummary, ViewportOwner,
};
use crate::state::StateMachine;
use chrono::Utc;
use portable_pty::{MasterPty, PtySystem};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tracing;

/// 会话编排器。管理会话生命周期并通知状态机。
pub struct SessionManager {
    store: Box<dyn SessionStore>,
    /// session_id → child PID 映射，用于 kill_session
    child_pids: tokio::sync::Mutex<HashMap<String, u32>>,
    /// session_id → PTY writer + OutputFanout，供 attach_pty 使用
    attach_handles: tokio::sync::Mutex<HashMap<String, PtyAttachHandle>>,
    /// session_id → PTY master，供 resize 真正作用到底层伪终端。
    pty_masters:
        tokio::sync::Mutex<HashMap<String, Arc<std::sync::Mutex<Box<dyn MasterPty + Send>>>>>,
    /// 创建互斥锁：防止 check+insert 竞态导致超过 SESSION_LIMIT
    create_mutex: tokio::sync::Mutex<()>,
    /// 远程开关互斥锁：防止 check+set 竞态导致超过 REMOTE_LIMIT
    remote_mutex: tokio::sync::Mutex<()>,
    summary_notifier: std::sync::Mutex<Option<mpsc::UnboundedSender<(String, String)>>>,
}

/// 全局会话数量上限
pub const SESSION_LIMIT: usize = 10;
/// 同时开启远程控制的会话数量上限
pub const REMOTE_LIMIT: usize = 10;
const HISTORY_RESUME_OUTPUT_TAIL_BYTES: usize = 8 * 1024;

/// Explains why the CLI process is being launched. This must be explicit: CLI
/// arguments alone are not a reliable indicator of business intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLaunchKind {
    Standard,
    LocalHistoryResume,
}

struct ProcessExit {
    exit_code: Option<u32>,
    output_tail: String,
}

fn append_output_tail(tail: &mut Vec<u8>, output: &[u8]) {
    tail.extend_from_slice(output);
    if tail.len() > HISTORY_RESUME_OUTPUT_TAIL_BYTES {
        let excess = tail.len() - HISTORY_RESUME_OUTPUT_TAIL_BYTES;
        tail.drain(..excess);
    }
}

fn history_resume_session_not_found(output: &str) -> bool {
    let output = output.to_ascii_lowercase();
    [
        "no session found",
        "session not found",
        "could not find session",
        "cannot find session",
        "unknown session",
        "session does not exist",
        "no conversation found",
        "conversation not found",
        "unknown conversation",
        "conversation does not exist",
    ]
    .iter()
    .any(|pattern| output.contains(pattern))
}

fn session_exit_reason(launch_kind: SessionLaunchKind, exit: &ProcessExit) -> &'static str {
    if launch_kind != SessionLaunchKind::LocalHistoryResume {
        return "process_exit";
    }
    if history_resume_session_not_found(&exit.output_tail) {
        return "history_resume_not_found";
    }
    // Exit 130 is the conventional Ctrl-C status. It is a normal user action,
    // not evidence that the saved history could not be reopened.
    if exit.exit_code.is_some_and(|code| code != 0 && code != 130) {
        return "history_resume_failed";
    }
    "process_exit"
}

impl SessionManager {
    pub fn new(store: Box<dyn SessionStore>) -> Self {
        Self {
            store,
            child_pids: tokio::sync::Mutex::new(HashMap::new()),
            attach_handles: tokio::sync::Mutex::new(HashMap::new()),
            pty_masters: tokio::sync::Mutex::new(HashMap::new()),
            create_mutex: tokio::sync::Mutex::new(()),
            remote_mutex: tokio::sync::Mutex::new(()),
            summary_notifier: std::sync::Mutex::new(None),
        }
    }

    pub fn set_summary_notifier(&self, notifier: mpsc::UnboundedSender<(String, String)>) {
        *self
            .summary_notifier
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(notifier);
    }

    /// 创建新会话（收到 start_session 后调用）。
    ///
    /// 内部持有 create_mutex，保证 count+insert 原子性，防止并发超限。
    /// 返回 `Err(AgentError::SessionLimit)` 当会话数已达 SESSION_LIMIT。
    pub async fn create(
        &self,
        nid: String,
        source: String,
        tool: String,
        profile: Option<String>,
        cwd: String,
        kind: SessionKind,
    ) -> Result<ManagedSession> {
        let _guard = self.create_mutex.lock().await;

        // Relay 会话不受 10 会话限制（本地 PTY 无限制）
        if kind == SessionKind::Native {
            let active = self.store.count_active().await?;
            if active >= SESSION_LIMIT {
                return Err(AgentError::SessionLimit {
                    current: active,
                    max: SESSION_LIMIT,
                });
            }
        }

        let viewport_owner = ViewportOwner::from_source(&source);
        let session = ManagedSession {
            kind,
            nid: nid.clone(),
            source,
            tool,
            profile,
            cwd,
            cols: 80,
            rows: 24,
            viewport_owner,
            created_at: Utc::now(),
            status: SessionStatus::Created,
            last_input: Arc::new(std::sync::Mutex::new(String::new())),
            last_output_snippet: Arc::new(std::sync::Mutex::new(String::new())),
            display_summary: Arc::new(std::sync::Mutex::new(None)),
            summary_input_buffer: Arc::new(std::sync::Mutex::new(String::new())),
            ended_reported: Arc::new(AtomicBool::new(false)),
            remote_enabled: Arc::new(AtomicBool::new(false)),
            relay_inputs: Arc::new(std::sync::Mutex::new(Vec::new())),
        };

        self.store.insert(session.clone()).await?;
        tracing::info!(nid = %nid, kind = ?kind, "会话已创建");
        Ok(session)
    }

    /// 标记会话为运行中。
    pub async fn mark_running(&self, nid: &str) -> Result<()> {
        let mut session = self
            .store
            .get(nid)
            .await?
            .ok_or_else(|| AgentError::SessionNotFound(nid.to_string()))?;
        session.status = SessionStatus::Running;
        self.store.insert(session).await?;
        Ok(())
    }

    /// 结束会话。
    pub async fn end(&self, nid: &str) -> Result<Option<ManagedSession>> {
        let mut session = match self.store.get(nid).await? {
            Some(s) => s,
            None => return Ok(None),
        };
        session.status = SessionStatus::Ended;
        self.store.insert(session.clone()).await?;
        tracing::info!(nid = %nid, "会话已结束");
        Ok(Some(session))
    }

    /// 强制终止会话（SIGKILL + 清理）。
    ///
    /// 安全措施：
    /// 1. 发 SIGKILL 前用 kill(pid, 0) 验活，防止 PID 重用误杀无关进程
    /// 2. remove 语义：PID 取出即删，不会重复 kill
    pub async fn kill_session(&self, nid: &str) -> Result<()> {
        tracing::info!(nid = %nid, "强制终止会话");

        // Kill PTY child process by PID（remove 保证只 kill 一次）
        if let Some(pid) = self.child_pids.lock().await.remove(nid) {
            #[cfg(unix)]
            unsafe {
                // kill(pid, 0) 不发送信号，仅检查进程是否存在。
                // 若进程已退出且 PID 被 OS 回收重用，此处返回 -1 (ESRCH)，
                // 跳过 SIGKILL 避免误杀不相关进程。
                if libc::kill(pid as i32, 0) == 0 {
                    libc::kill(pid as i32, libc::SIGKILL);
                    tracing::info!(pid = pid, "已终止子进程");
                } else {
                    tracing::info!(pid = pid, "进程已退出，跳过 SIGKILL（避免 PID 重用）");
                }
            }
        }

        // 清理 attach handle + proxy socket
        self.attach_handles.lock().await.remove(nid);
        self.pty_masters.lock().await.remove(nid);
        let sock = pty_sock_path(nid);
        if sock.exists() {
            let _ = std::fs::remove_file(&sock);
        }
        // 此路径已强制终止 PTY；删除完整回放日志，避免已结束会话占用磁盘。
        remove_replay_log(nid);
        // 清理持久化文件
        super::persistence::delete_session_record(nid);

        let _ = self.end(nid).await;
        Ok(())
    }

    /// 上报 session_ended 到云端，仅允许执行一次。
    pub async fn report_session_ended(
        &self,
        nid: &str,
        reason: &str,
    ) -> Result<Option<crate::proto::OutgoingMessage>> {
        let mut session = match self.store.get(nid).await? {
            Some(s) => s,
            None => return Ok(None),
        };
        if !session.mark_ended_reported() {
            return Ok(None);
        }

        let nid = session.nid.clone();

        session.status = SessionStatus::Ended;
        self.store.insert(session).await?;
        // Relay sessions do not own a local PTY reader, so this is their only
        // common terminal lifecycle exit. Native sessions may already have
        // removed the log after their final PTY flush; deletion is idempotent.
        remove_replay_log(&nid);
        // 清理持久化文件
        super::persistence::delete_session_record(&nid);
        Ok(Some(crate::proto::OutgoingMessage::SessionEnded {
            session_nid: nid,
            reason: reason.to_string(),
        }))
    }

    /// 如果会话是 Ended 状态，重置为 Running（供 re-enable relay 使用）。
    pub async fn reactivate_if_ended(&self, nid: &str) -> Result<()> {
        if let Some(mut s) = self.store.get(nid).await? {
            if s.status == SessionStatus::Ended {
                s.status = SessionStatus::Running;
                self.store.insert(s).await?;
            }
        }
        Ok(())
    }

    /// 原子检查 + 开启远程控制。防止并发调用突破 REMOTE_LIMIT。
    /// 返回 `Err(AgentError::SessionLimit)` 如果已达上限。
    pub async fn try_enable_remote(&self, nid: &str) -> Result<()> {
        let _guard = self.remote_mutex.lock().await;
        let count = self.store.count_remote_enabled().await?;
        if count >= REMOTE_LIMIT {
            return Err(AgentError::SessionLimit {
                current: count,
                max: REMOTE_LIMIT,
            });
        }
        let session = self
            .store
            .get(nid)
            .await?
            .ok_or_else(|| AgentError::SessionNotFound(nid.to_string()))?;
        session.remote_enabled.store(true, Ordering::Relaxed);
        self.store.insert(session.clone()).await?;
        // 同步 session.json
        if let Some(pid) = self.child_pids.lock().await.get(nid).copied() {
            super::persistence::update_session_record(&session, pid);
        }
        tracing::info!(nid = %nid, count = count + 1, "远程控制已开启");
        Ok(())
    }

    /// 设置会话的远程控制开关。
    pub async fn set_remote_enabled(&self, nid: &str, enabled: bool) -> Result<()> {
        let session = self
            .store
            .get(nid)
            .await?
            .ok_or_else(|| AgentError::SessionNotFound(nid.to_string()))?;
        session.remote_enabled.store(enabled, Ordering::Relaxed);
        self.store.insert(session.clone()).await?;
        // 同步 session.json
        if let Some(pid) = self.child_pids.lock().await.get(nid).copied() {
            super::persistence::update_session_record(&session, pid);
        }
        tracing::info!(nid = %nid, enabled = enabled, "远程控制状态已更新");
        Ok(())
    }

    /// Revoke every locally persisted remote exposure after token revocation or self-unbind.
    pub async fn disable_all_remote(&self) -> Result<usize> {
        let sessions = self.list().await?;
        let mut disabled = 0;
        let mut first_error = None;
        for session in sessions
            .into_iter()
            .filter(|session| session.remote_enabled)
        {
            match self.set_remote_enabled(&session.nid, false).await {
                Ok(()) => disabled += 1,
                Err(error) => {
                    tracing::error!(nid = %session.nid, error = %error, "关闭远程会话失败");
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(disabled), Err)
    }

    /// 存储 PTY writer + OutputFanout，供后续 `attach_pty` 使用。
    pub(crate) async fn store_attach_handle(&self, nid: &str, handle: PtyAttachHandle) {
        self.attach_handles
            .lock()
            .await
            .insert(nid.to_string(), handle);
    }

    /// 存储 PTY master，供 iOS / desktop resize 修改真实 PTY 尺寸。
    async fn store_pty_master(
        &self,
        nid: &str,
        master: Arc<std::sync::Mutex<Box<dyn MasterPty + Send>>>,
    ) {
        self.pty_masters
            .lock()
            .await
            .insert(nid.to_string(), master);
    }

    /// 创建 pty.sock 并桥接 PTY I/O，返回 socket 路径。
    ///
    /// 输出方向：订阅 OutputFanout → pty.sock（不走 PTY dup，避免分食输出）
    /// 输入方向：pty.sock → PTY writer
    pub async fn attach_pty(&self, nid: &str) -> std::result::Result<PathBuf, String> {
        let sock_path = pty_sock_path(nid);

        // 确保目录存在
        if let Some(parent) = sock_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {}", e))?;
        }

        // 清理旧 socket
        if sock_path.exists() {
            std::fs::remove_file(&sock_path).map_err(|e| format!("remove old sock: {}", e))?;
        }

        let listener =
            UnixListener::bind(&sock_path).map_err(|e| format!("bind pty.sock: {}", e))?;

        // 权限 0600
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o600));
        }

        let handles = self.attach_handles.lock().await;
        let handle = handles
            .get(nid)
            .ok_or_else(|| "session not found".to_string())?;

        // 从 OutputFanout 订阅输出（不 dup PTY reader，避免分食问题）
        let mut output_rx = handle.fanout.register_subscriber();
        let pty_writer = handle.writer.clone();
        drop(handles);
        let summary_session = self
            .store
            .get(nid)
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "session not found".to_string())?;
        let summary_notifier = self
            .summary_notifier
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        let sid = nid.to_string();
        tokio::spawn(async move {
            let (stream, _addr) = match listener.accept().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(session_id = %sid, error = %e, "pty.sock accept failed");
                    return;
                }
            };
            drop(listener);

            let (mut sock_reader, mut sock_writer) = stream.into_split();

            if let Some(history) = OutputFanout::replay_log(&sid) {
                if sock_writer.write_all(&history).await.is_err() {
                    tracing::debug!(session_id = %sid, "pty.sock client disconnected during replay");
                    return;
                }
            }

            // OutputFanout → pty.sock（后台 task）
            let sid_clone = sid.clone();
            tokio::spawn(async move {
                while let Some(data) = output_rx.recv().await {
                    if sock_writer.write_all(&data).await.is_err() {
                        break;
                    }
                }
                tracing::debug!(session_id = %sid_clone, "output→socket writer exited");
            });

            // pty.sock → PTY writer（当前 task，连接断开即退出）
            let mut buf = vec![0u8; 16384];
            loop {
                match sock_reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if let Ok(text) = std::str::from_utf8(&buf[..n]) {
                            if let Some(summary) =
                                summary_session.record_display_summary_input(text)
                            {
                                if let Some(notifier) = summary_notifier.as_ref() {
                                    let _ = notifier.send((sid.clone(), summary));
                                }
                            }
                        }
                        let mut w = pty_writer.lock().await;
                        use std::io::Write;
                        if w.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            tracing::debug!(session_id = %sid, "PTY proxy client disconnected");
        });

        Ok(sock_path)
    }

    /// 删除会话。
    pub async fn remove(&self, nid: &str) -> Result<Option<ManagedSession>> {
        self.store.remove(nid).await
    }

    /// 获取会话。
    pub async fn get(&self, nid: &str) -> Result<Option<ManagedSession>> {
        self.store.get(nid).await
    }

    /// 列出所有会话。
    pub async fn list(&self) -> Result<Vec<SessionSummary>> {
        self.store.list().await
    }

    /// 活跃会话数量（非 Ended 状态）。
    pub async fn active_count(&self) -> Result<usize> {
        self.store.count_active().await
    }

    /// 已开启远程控制的会话数量。
    pub async fn count_remote_enabled(&self) -> Result<usize> {
        self.store.count_remote_enabled().await
    }

    /// 获取所有会话 nanoid 列表。
    pub async fn all_nids(&self) -> Result<Vec<String>> {
        let summaries = self.store.list().await?;
        Ok(summaries.into_iter().map(|s| s.nid).collect())
    }

    /// 获取会话的 CLI 子进程 PID（用于进程存活检测）。
    pub async fn get_child_pid(&self, nid: &str) -> Option<u32> {
        self.child_pids.lock().await.get(nid).copied()
    }

    /// 设置会话的 CLI 子进程 PID（desktop Relay session 补传 PID 时使用）。
    pub async fn set_child_pid(&self, nid: &str, pid: u32) {
        if pid > 0 {
            self.child_pids.lock().await.insert(nid.to_string(), pid);
            tracing::info!(nid = %nid, pid = pid, "PID 已记录");
        }
    }

    /// 清理 desktop-owned Relay 的 PID 跟踪；Relay PTY 已由桌面侧结束。
    pub async fn clear_child_pid(&self, nid: &str) {
        self.child_pids.lock().await.remove(nid);
    }

    /// Returns the first completed user input once, for publication as the
    /// active remote session's compact display summary.
    pub async fn record_display_summary_input(
        &self,
        nid: &str,
        text: &str,
    ) -> Result<Option<String>> {
        let session = self
            .store
            .get(nid)
            .await?
            .ok_or_else(|| AgentError::SessionNotFound(nid.to_string()))?;
        let summary = session.record_display_summary_input(text);
        if let Some(value) = &summary {
            if let Some(notifier) = self
                .summary_notifier
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .as_ref()
            {
                let _ = notifier.send((nid.to_string(), value.clone()));
            }
        }
        Ok(summary)
    }

    /// Queue remote input for a desktop-owned Relay session.
    pub async fn queue_relay_input(&self, nid: &str, text: String) -> Result<()> {
        let session = self
            .store
            .get(nid)
            .await?
            .ok_or_else(|| AgentError::SessionNotFound(nid.to_string()))?;
        if session.kind != SessionKind::Relay {
            return Err(AgentError::Other(format!("session is not Relay: {}", nid)));
        }
        session.record_input(&text);
        {
            let mut queue = session
                .relay_inputs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            queue.push((Utc::now().timestamp_millis(), text));
        }
        self.store.insert(session).await?;
        Ok(())
    }

    /// Drain queued remote input for a desktop-owned Relay session.
    pub async fn take_relay_inputs(&self, nid: &str) -> Result<Vec<String>> {
        let session = self
            .store
            .get(nid)
            .await?
            .ok_or_else(|| AgentError::SessionNotFound(nid.to_string()))?;
        let mut queue = session
            .relay_inputs
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Ok(queue.drain(..).map(|(_, text)| text).collect())
    }

    /// 更新终端尺寸。
    pub async fn resize(&self, nid: &str, cols: u16, rows: u16) -> Result<()> {
        self.resize_pty(nid, cols, rows).await?;
        let mut session = self
            .store
            .get(nid)
            .await?
            .ok_or_else(|| AgentError::SessionNotFound(nid.to_string()))?;
        session.cols = cols;
        session.rows = rows;
        self.store.insert(session).await?;
        Ok(())
    }

    /// 更新终端尺寸，并标记当前视口主控端。
    pub async fn resize_from_source(
        &self,
        nid: &str,
        cols: u16,
        rows: u16,
        owner: ViewportOwner,
    ) -> Result<()> {
        self.resize_pty(nid, cols, rows).await?;
        let mut session = self
            .store
            .get(nid)
            .await?
            .ok_or_else(|| AgentError::SessionNotFound(nid.to_string()))?;
        session.cols = cols;
        session.rows = rows;
        session.viewport_owner = owner;
        self.store.insert(session).await?;
        Ok(())
    }

    async fn resize_pty(&self, nid: &str, cols: u16, rows: u16) -> Result<()> {
        let master = self.pty_masters.lock().await.get(nid).cloned();
        let Some(master) = master else {
            tracing::debug!(nid = %nid, cols = cols, rows = rows, "PTY master 不存在，仅更新会话尺寸元数据");
            return Ok(());
        };

        let size = portable_pty::PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        };
        master
            .lock()
            .map_err(|_| AgentError::Other("PTY master lock poisoned".to_string()))?
            .resize(size)
            .map_err(|e| AgentError::Other(format!("pty resize failed: {}", e)))?;
        tracing::info!(nid = %nid, cols = cols, rows = rows, "PTY 已调整尺寸");
        Ok(())
    }

    /// 标记当前视口主控端，不改变 PTY 尺寸。
    pub async fn set_viewport_owner(&self, nid: &str, owner: ViewportOwner) -> Result<()> {
        let mut session = self
            .store
            .get(nid)
            .await?
            .ok_or_else(|| AgentError::SessionNotFound(nid.to_string()))?;
        session.viewport_owner = owner;
        self.store.insert(session).await?;
        Ok(())
    }

    // ── PTY session lifecycle ────────────────────────────────

    /// 创建 PTY 会话并启动 CLI 进程。返回 OutputFanout 用于接收 PTY 输出。
    pub async fn start_session(
        self: Arc<Self>,
        nid: &str,
        tool: &str,
        profile: Option<&str>,
        cwd: &str,
        cols: u16,
        rows: u16,
        wss_tx: mpsc::UnboundedSender<String>,
        ipc_tx: mpsc::UnboundedSender<String>,
        merger: std::sync::Arc<InputMerger>,
        remote_enabled: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> std::result::Result<OutputFanout, String> {
        self.start_session_with_args(
            nid,
            tool,
            profile,
            cwd,
            cols,
            rows,
            &[],
            SessionLaunchKind::Standard,
            wss_tx,
            ipc_tx,
            merger,
            remote_enabled,
        )
        .await
    }

    /// 创建 PTY 会话，并将可信的原生 CLI 参数附加到二进制调用。
    /// 参数始终通过 `CommandBuilder` 逐项传递，绝不经 shell 解释。
    pub async fn start_session_with_args(
        self: Arc<Self>,
        nid: &str,
        tool: &str,
        profile: Option<&str>,
        cwd: &str,
        cols: u16,
        rows: u16,
        cli_args: &[String],
        launch_kind: SessionLaunchKind,
        wss_tx: mpsc::UnboundedSender<String>,
        ipc_tx: mpsc::UnboundedSender<String>,
        merger: std::sync::Arc<InputMerger>,
        remote_enabled: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> std::result::Result<OutputFanout, String> {
        // 1. 查找 CLI 二进制
        let binary = resolve_tool_path(tool)?;

        // 2. 读 profile env vars
        let env_vars =
            if let Some(p) = profile {
                match kn_common::profile::get_env_cmd(p) {
                    Ok(v) => Some(v.env),
                    Err(e) => {
                        let _ = wss_tx.send(serde_json::json!({
                        "type": "error_notify",
                        "data": { "code": "config_parse_error", "message": format!("{}", e) }
                    }).to_string());
                        return Err(format!("config_parse_error: {}", e));
                    }
                }
            } else {
                None
            };

        // 3. openpty
        let pty_system = portable_pty::NativePtySystem::default();
        let size = portable_pty::PtySize {
            rows,
            cols,
            pixel_width: 0,
            pixel_height: 0,
        };
        let pair = pty_system.openpty(size).map_err(|e| {
            let _ = wss_tx.send(
                serde_json::json!({
                    "type": "error_notify",
                    "data": { "code": "pty_alloc_failed", "message": format!("{}", e) }
                })
                .to_string(),
            );
            format!("pty_alloc_failed: {}", e)
        })?;

        // 4. Tool 预处理。Codex auth 短窗口切换必须发生在 openpty 成功后，
        // 避免 PTY 分配失败时留下临时 auth.json。
        let mut prep = prepare_tool_env(tool, &env_vars, profile)?;

        // 5. spawn CLI. Real CLI binaries are spawned directly so the shell
        // never falls back to interpreting Mach-O bytes as a script.
        let mut cmd = if tool == "bash" {
            let shell = std::env::var("SHELL").unwrap_or_else(|_| binary.clone());
            let mut shell_cmd = portable_pty::CommandBuilder::new(&shell);
            shell_cmd.args(["-i", "-l"]);
            shell_cmd
        } else {
            let mut cli_cmd = portable_pty::CommandBuilder::new(&binary);
            cli_cmd.args(&prep.extra_args);
            cli_cmd.args(cli_args);
            cli_cmd
        };
        if !cwd.is_empty() {
            cmd.cwd(cwd);
        }

        for (k, v) in std::env::vars() {
            if prep.env_removals.iter().any(|name| name == &k) {
                continue;
            }
            cmd.env(&k, &v);
        }
        if let Some(ref ev) = env_vars {
            for (k, v) in ev {
                if prep.env_removals.iter().any(|name| name == k) {
                    continue;
                }
                cmd.env(k, v);
            }
        }
        cmd.env("KN_SESSION_ID", nid);
        cmd.env("KN_CLI_TOOL", tool);
        if let Some(profile) = profile {
            cmd.env("KN_PROFILE", profile);
        }
        cmd.env("KN_WORKING_DIR", cwd);
        // PATH 补齐 + TERM
        if cfg!(target_os = "macos") {
            let current_path = std::env::var("PATH").unwrap_or_default();
            let extra = [
                "/opt/homebrew/bin",
                "/opt/homebrew/sbin",
                "/usr/local/bin",
                "/usr/local/sbin",
            ];
            let missing: Vec<&str> = extra
                .iter()
                .filter(|p| !current_path.split(':').any(|seg| seg == **p))
                .copied()
                .collect();
            if !missing.is_empty() {
                cmd.env("PATH", format!("{}:{}", current_path, missing.join(":")));
            }
        }
        cmd.env("TERM", "xterm-256color");
        cmd.env("COLORTERM", "truecolor");
        cmd.env("TERM_PROGRAM", "kn");
        if std::env::var_os("LANG").is_none() {
            cmd.env("LANG", "en_US.UTF-8");
        }

        let mut child = pair.slave.spawn_command(cmd).map_err(|e| {
            if let Some(auth_restore) = prep.auth_restore.as_mut() {
                let _ = auth_restore.restore_after_start();
            }
            let _ = wss_tx.send(
                serde_json::json!({
                    "type": "error_notify",
                    "data": { "code": "shell_spawn_failed", "message": format!("{}", e) }
                })
                .to_string(),
            );
            format!("shell_spawn_failed: {}", e)
        })?;
        if let Some(auth_restore) = prep.auth_restore.as_mut() {
            std::thread::sleep(super::codex_auth::restore_grace_period());
            if let Err(error) = auth_restore.restore_after_start() {
                tracing::warn!(error=%error, "Codex auth restore after start failed; will retry on session exit");
            }
        }
        let mut auth_restore_on_exit = prep.auth_restore;
        drop(pair.slave);

        // 6. 创建 I/O 通道 + session 生命周期令牌
        let session_cancel = tokio_util::sync::CancellationToken::new();
        let master = Arc::new(std::sync::Mutex::new(pair.master));
        let mut reader = master
            .lock()
            .map_err(|_| "master lock poisoned".to_string())?
            .try_clone_reader()
            .map_err(|e| format!("clone reader: {}", e))?;
        let writer: std::sync::Arc<tokio::sync::Mutex<Box<dyn std::io::Write + Send>>> =
            std::sync::Arc::new(tokio::sync::Mutex::new(Box::new(
                master
                    .lock()
                    .map_err(|_| "master lock poisoned".to_string())?
                    .take_writer()
                    .map_err(|e| format!("take writer: {}", e))?,
            )));

        // clone 在 remote_enabled 被 move 进 OutputFanout::new 之前
        let remote_enabled_for_end = remote_enabled.clone();

        // 7. OutputFanout（带取消令牌，session 结束后停止定时器）
        let fanout = OutputFanout::new(
            nid.to_string(),
            Some(wss_tx),
            Some(ipc_tx),
            session_cancel.clone(),
            remote_enabled,
        );
        self.mark_running(nid).await.map_err(|e| e.to_string())?;

        // 存储 fanout + writer，供 attach_pty 使用
        self.store_attach_handle(
            nid,
            PtyAttachHandle {
                writer: writer.clone(),
                fanout: fanout.clone(),
            },
        )
        .await;
        self.store_pty_master(nid, master.clone()).await;

        // 8. PTY stdout 读取线程（spawn_blocking）+ child 回收 (B1)
        let child_pid = child.process_id().unwrap_or_else(|| {
            tracing::error!(session_id = %nid, "PTY child has no PID — kill_session will be a no-op");
            0
        });
        let fanout_clone = fanout.clone();
        let reader_cancel = session_cancel.clone();
        let sid = nid.to_string();
        let (end_tx, mut end_rx) = mpsc::unbounded_channel::<ProcessExit>();
        let end_wss_tx = fanout.inner.wss_tx.clone();
        let sid_for_end = sid.clone();
        let sessions_for_end = self.clone();
        // PID 清理：子进程退出后通过 self (Arc) 访问 child_pids 移除
        let self_for_pid_cleanup = self.clone();
        let cleanup_nid = nid.to_string();
        tokio::spawn(async move {
            tracing::debug!(session_id = %sid_for_end, "session_ended 监听任务已启动");
            while let Some(exit) = end_rx.recv().await {
                tracing::info!(session_id = %sid_for_end, "收到 session_ended 信号，准备上报");
                let reason = session_exit_reason(launch_kind, &exit);
                if let Some(msg) = sessions_for_end
                    .report_session_ended(&sid_for_end, reason)
                    .await
                    .ok()
                    .flatten()
                {
                    // 只对开启了远程的会话发送 session_ended 到云端
                    let is_remote = remote_enabled_for_end
                        .as_ref()
                        .map(|f| f.load(std::sync::atomic::Ordering::Relaxed))
                        .unwrap_or(false);
                    if is_remote {
                        if let Some(tx) = end_wss_tx.as_ref() {
                            let _ = tx.send(msg.to_json());
                            tracing::info!(session_id = %sid_for_end, "session_ended 已发送到 Cloud");
                        } else {
                            tracing::warn!(session_id = %sid_for_end, "wss_tx 不可用，无法发送 session_ended");
                        }
                    }
                } else {
                    tracing::warn!(session_id = %sid_for_end, "report_session_ended 返回 None，可能已经上报过或 session 不存在");
                }
            }
            tracing::debug!(session_id = %sid_for_end, "session_ended 监听任务退出");
        });
        tokio::task::spawn_blocking(move || {
            let mut buf = [0u8; 16384];
            let mut output_tail = Vec::with_capacity(HISTORY_RESUME_OUTPUT_TAIL_BYTES);
            let result = loop {
                match reader.read(&mut buf) {
                    Ok(0) => break Ok(()),
                    Ok(n) => {
                        append_output_tail(&mut output_tail, &buf[..n]);
                        fanout_clone.broadcast(&buf[..n]);
                    }
                    Err(e) => break Err(e),
                }
            };
            // Do this before cancellation stops the periodic flush timer: a
            // CLI's final resume error is often its only output.
            fanout_clone.flush_pending();
            // 等待子进程退出（回收僵尸进程）
            let exit_code = match child.wait() {
                Ok(status) => {
                    tracing::info!(session_id=%sid, exit_code=%status.exit_code(), "PTY 进程已退出");
                    Some(status.exit_code())
                }
                Err(e) => {
                    tracing::warn!(session_id=%sid, error=%e, "PTY wait 失败");
                    None
                }
            };
            if let Some(auth_restore) = auth_restore_on_exit.as_mut() {
                if let Err(error) = auth_restore.restore_on_exit() {
                    tracing::warn!(session_id=%sid, error=%error, "Codex auth restore on exit failed");
                }
            }
            // 子进程已退出，立即从 child_pids 移除，防止 kill_session 操作已回收的 PID
            {
                let removed = self_for_pid_cleanup
                    .child_pids
                    .blocking_lock()
                    .remove(&cleanup_nid);
                tracing::debug!(session_id=%cleanup_nid, pid_removed=?removed, "PID 已从 child_pids 清理");
            }
            {
                self_for_pid_cleanup
                    .pty_masters
                    .blocking_lock()
                    .remove(&cleanup_nid);
                tracing::debug!(session_id=%cleanup_nid, "PTY master 已清理");
            }
            // PTY 已 EOF 且最终输出已 flush；现在才可以删除完整回放日志。
            remove_replay_log(&cleanup_nid);
            match result {
                Ok(()) => tracing::info!(session_id=%sid, "PTY EOF"),
                Err(e) => tracing::warn!(session_id=%sid, error=%e, "PTY read error"),
            }
            reader_cancel.cancel();
            tracing::info!(session_id=%sid, "PTY 进程已退出，准备发送 session_ended 信号");
            let exit = ProcessExit {
                exit_code,
                output_tail: String::from_utf8_lossy(&output_tail).into_owned(),
            };
            match end_tx.send(exit) {
                Ok(_) => tracing::info!(session_id=%sid, "session_ended 信号已发送到监听任务"),
                Err(e) => tracing::error!(session_id=%sid, error=%e, "发送 session_ended 信号失败"),
            }
        });

        // 9. PTY stdin 写入循环（B2：session_cancel 时退出）
        let notify = merger.register_session(nid).await;
        let writer_clone = writer.clone();
        let writer_cancel = session_cancel.clone();
        let sid = nid.to_string();
        let merger_clone = merger.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = notify.notified() => {
                        while let Some(msg) = merger_clone.pop(&sid).await {
                            let txt = msg.text.clone();
                            let len = txt.len();
                            let preview = if txt.chars().count() <= 50 { txt.clone() } else { format!("{}...", txt.chars().take(50).collect::<String>()) };
                            let mut w = writer_clone.lock().await;
                            match w.write_all(msg.text.as_bytes()) {
                                Ok(_) => tracing::info!(session_id = %sid, len = len, text = %preview, "⌨️  [PTY-IN] 写入 PTY stdin"),
                                Err(e) => tracing::error!(session_id = %sid, error = %e, "⌨️  [PTY-IN] 写入失败"),
                            }
                        }
                    }
                    _ = writer_cancel.cancelled() => {
                        tracing::debug!(session_id=%sid, "stdin writer 退出");
                        break;
                    }
                }
            }
            merger_clone.unregister_session(&sid).await;
        });

        // 存储 PID 供 kill_session 使用（跳过 0，防止误杀进程组）
        if child_pid > 0 {
            self.child_pids
                .lock()
                .await
                .insert(nid.to_string(), child_pid);
        }

        // 持久化：写 session.json，agent 重启后可恢复
        if child_pid > 0 {
            if let Ok(Some(session)) = self.get(nid).await {
                if let Err(e) = super::persistence::write_session_record(&session, child_pid) {
                    tracing::warn!(nid = %nid, error = %e, "写 session.json 失败");
                }
            }
        }

        Ok(fanout)
    }

    // ── Checkpoint (DEPRECATED: 由 CLI 心跳 + Redis 替代) ───

    /// @deprecated 由 cli_heartbeat 心跳 + Redis 替代。保留方法签名兼容旧调用。
    #[deprecated(note = "use cli_heartbeat instead")]
    pub async fn save_checkpoint(&self, _nid: &str) -> std::result::Result<(), String> {
        Ok(())
    }

    /// @deprecated 由 SessionHeartbeatMonitor 替代。
    #[deprecated(note = "use cli_heartbeat instead")]
    pub fn start_checkpoint_loop(_sm: Arc<SessionManager>, _state: Arc<StateMachine>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::store::MemorySessionStore;

    #[test]
    fn history_resume_exit_is_classified_from_cli_evidence() {
        let missing = ProcessExit {
            exit_code: Some(1),
            output_tail: "Error: no session found with ID abc".to_string(),
        };
        assert_eq!(
            session_exit_reason(SessionLaunchKind::LocalHistoryResume, &missing),
            "history_resume_not_found"
        );

        let failed = ProcessExit {
            exit_code: Some(1),
            output_tail: "authentication failed".to_string(),
        };
        assert_eq!(
            session_exit_reason(SessionLaunchKind::LocalHistoryResume, &failed),
            "history_resume_failed"
        );
    }

    #[test]
    fn normal_and_user_interrupted_history_sessions_keep_process_exit_reason() {
        let user_interrupted = ProcessExit {
            exit_code: Some(130),
            output_tail: String::new(),
        };
        assert_eq!(
            session_exit_reason(SessionLaunchKind::LocalHistoryResume, &user_interrupted),
            "process_exit"
        );
        let ordinary_failure = ProcessExit {
            exit_code: Some(1),
            output_tail: "no session found".to_string(),
        };
        assert_eq!(
            session_exit_reason(SessionLaunchKind::Standard, &ordinary_failure),
            "process_exit"
        );
    }

    #[test]
    fn output_tail_keeps_the_most_recent_bytes() {
        let mut tail = b"1234".to_vec();
        append_output_tail(&mut tail, &vec![b'x'; HISTORY_RESUME_OUTPUT_TAIL_BYTES]);
        assert_eq!(tail.len(), HISTORY_RESUME_OUTPUT_TAIL_BYTES);
        assert!(tail.iter().all(|byte| *byte == b'x'));
    }

    fn test_manager() -> SessionManager {
        SessionManager::new(Box::new(MemorySessionStore::new()))
    }

    #[tokio::test]
    async fn resize_from_ios_updates_size_and_owner() {
        let manager = test_manager();
        manager
            .create(
                "s_ios".into(),
                "desktop".into(),
                "bash".into(),
                None,
                "/tmp".into(),
                SessionKind::Native,
            )
            .await
            .expect("create session");

        manager
            .resize_from_source("s_ios", 52, 18, ViewportOwner::Ios)
            .await
            .expect("resize from ios");

        let session = manager.get("s_ios").await.unwrap().unwrap();
        assert_eq!(session.cols, 52);
        assert_eq!(session.rows, 18);
        assert_eq!(session.viewport_owner, ViewportOwner::Ios);
    }

    #[tokio::test]
    async fn desktop_input_marks_owner_without_resizing() {
        let manager = test_manager();
        manager
            .create(
                "s_desktop".into(),
                "ios".into(),
                "bash".into(),
                None,
                "/tmp".into(),
                SessionKind::Native,
            )
            .await
            .expect("create session");
        manager
            .resize_from_source("s_desktop", 50, 20, ViewportOwner::Ios)
            .await
            .expect("resize from ios");

        manager
            .set_viewport_owner("s_desktop", ViewportOwner::Desktop)
            .await
            .expect("set desktop owner");

        let session = manager.get("s_desktop").await.unwrap().unwrap();
        assert_eq!(session.cols, 50);
        assert_eq!(session.rows, 20);
        assert_eq!(session.viewport_owner, ViewportOwner::Desktop);
    }

    #[tokio::test]
    async fn relay_inputs_are_queued_for_desktop_polling() {
        let manager = test_manager();
        manager
            .create(
                "s_relay".into(),
                "desktop".into(),
                "claude".into(),
                Some("work".into()),
                "/tmp".into(),
                SessionKind::Relay,
            )
            .await
            .expect("create relay session");

        manager
            .queue_relay_input("s_relay", "hello from ios\n".into())
            .await
            .expect("queue input");

        assert_eq!(
            manager
                .take_relay_inputs("s_relay")
                .await
                .expect("take inputs"),
            vec!["hello from ios\n".to_string()],
        );
        assert!(
            manager
                .take_relay_inputs("s_relay")
                .await
                .expect("take inputs again")
                .is_empty(),
            "polling should drain queued relay input"
        );
    }

    #[tokio::test]
    async fn relay_session_can_be_marked_running_for_local_panel_visibility() {
        let manager = test_manager();
        manager
            .create(
                "s_relay_running".into(),
                "desktop".into(),
                "claude".into(),
                Some("work".into()),
                "/tmp".into(),
                SessionKind::Relay,
            )
            .await
            .expect("create relay session");

        manager
            .mark_running("s_relay_running")
            .await
            .expect("mark running");

        let session = manager
            .get("s_relay_running")
            .await
            .expect("get session")
            .expect("session exists");
        assert_eq!(session.status, SessionStatus::Running);
    }
}
