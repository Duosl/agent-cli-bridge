use super::{build_argv, env_for, InvokeEvent};
use crate::agents::AgentProtocol;
use crate::parser;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

/// 运行 agent 并流式输出事件
pub async fn run_agent(
    bin: &str,
    agent_id: &str,
    protocol: AgentProtocol,
    prompt: &str,
    cwd: Option<PathBuf>,
    tx: mpsc::Sender<InvokeEvent>,
) -> Result<(), crate::Error> {
    let argv = build_argv(agent_id, None);
    let env = env_for(agent_id);

    let mut cmd = Command::new(bin);
    cmd.args(&argv)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdin(std::process::Stdio::piped());

    if let Some(cwd) = &cwd {
        cmd.current_dir(cwd);
    }

    // 设置环境变量
    for (key, value) in &env {
        cmd.env(key, value);
    }

    #[cfg(target_os = "windows")]
    {
        cmd.shell_create_arg_list(true);
    }

    let mut child = cmd
        .spawn()
        .map_err(|e| crate::Error::SpawnFailed(e.to_string()))?;

    // 先提取 stdout 和 stderr，避免部分移动问题
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    // 发送 Start 事件
    let _ = tx
        .send(InvokeEvent::Start {
            bin: bin.to_string(),
            argv,
        })
        .await;

    // 处理 stdin
    let prompt_via_argv = matches!(protocol, AgentProtocol::Argv);
    let prompt_via_message = matches!(protocol, AgentProtocol::ArgvMessage);

    if !prompt_via_argv && !prompt_via_message {
        if let Some(mut stdin) = child.stdin.take() {
            let prompt_bytes = prompt.as_bytes();
            let _ = stdin.write_all(prompt_bytes).await;
            let _ = stdin.shutdown().await;
        }
    }

    // 启动 stdout 和 stderr 读取任务
    let tx_stdout = tx.clone();
    let agent_id_stdout = agent_id.to_string();

    let stdout_handle = tokio::spawn(async move {
        read_stdout(stdout, &agent_id_stdout, tx_stdout).await;
    });

    let tx_stderr = tx.clone();
    let stderr_handle = tokio::spawn(async move {
        read_stderr(stderr, tx_stderr).await;
    });

    // 等待进程完成
    let status = child
        .wait()
        .await
        .map_err(|e| crate::Error::SpawnFailed(e.to_string()))?;

    // 等待读取任务完成
    let _ = stdout_handle.await;
    let _ = stderr_handle.await;

    // 发送 Done 事件
    let _ = tx
        .send(InvokeEvent::Done {
            code: status.code(),
        })
        .await;

    Ok(())
}

/// 读取 stdout 并解析事件
async fn read_stdout(
    stdout: Option<tokio::process::ChildStdout>,
    agent_id: &str,
    tx: mpsc::Sender<InvokeEvent>,
) {
    let stdout = match stdout {
        Some(s) => s,
        None => return,
    };

    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();
    let mut state = parser::ParseState::default();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.is_empty() {
            continue;
        }

        let events = parser::parse_line(agent_id, &line, &mut state);

        for event in events {
            match event {
                parser::ParseEvent::Delta(text) => {
                    let _ = tx.send(InvokeEvent::Delta { text }).await;
                }
                parser::ParseEvent::Html(text) => {
                    let _ = tx.send(InvokeEvent::Html { text }).await;
                }
                parser::ParseEvent::Meta { key, value } => {
                    let _ = tx.send(InvokeEvent::Meta { key, value }).await;
                }
                parser::ParseEvent::Noise => {}
            }
        }
    }
}

/// 读取 stderr
async fn read_stderr(
    stderr: Option<tokio::process::ChildStderr>,
    tx: mpsc::Sender<InvokeEvent>,
) {
    let stderr = match stderr {
        Some(s) => s,
        None => return,
    };

    let reader = BufReader::new(stderr);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if !line.is_empty() {
            let _ = tx.send(InvokeEvent::Stderr { text: line }).await;
        }
    }
}
