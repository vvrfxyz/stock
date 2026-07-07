#!/usr/bin/env bash
# 研究长任务标准发射器（2026-07-07，对抗审查裁定的"生存层"唯一真解）。
#
# 解决的处境：白天本机开发，把长任务丢到 253 上跑、ssh 断开人离场——
#   - nohup 下 OOM 是 SIGKILL，Python 层什么都打不出（atexit 也没用）；
#   - 日志文件名靠 ls -t logs/ 猜；
#   - 挂了没人知道，几小时后回来才发现白等。
# 解法（全部交给 systemd，Python 层刻意不做，见 research/progress.py 非目标清单）：
#   - 固定 unit 名 research-<tag>：journalctl --user -u research-<tag> 是标准查日志姿势；
#   - MemoryHigh 先软限流、MemoryMax 硬帽（护住 253 的 11G，daily pipeline 不被挤死）；
#   - OnFailure 挂 stock-research-notify@（user 级，转调既有 notify_failure.sh：
#     追加 logs/failures.log + 可选 STOCK_NOTIFY_URL webhook）；
#   - 死因不靠猜：systemctl --user show research-<tag> -p Result
#     （oom-kill / exit-code / signal 一眼分明）。
#
# 用法（253 上，wenruifeng 身份）：
#   scripts/run_research.sh ta-zoo -- .venv/bin/python -m research.evaluate --factors size ...
#
# 一次性准备（见 docs/deployment.md；仅 linger 需要 root/sudo，其余以 wenruifeng 身份）：
#   sudo loginctl enable-linger wenruifeng     # user manager 不随 ssh 会话死
#   mkdir -p ~/.config/systemd/user            # 以下以 wenruifeng 身份执行
#   cp deploy/systemd/user/stock-research-notify@.service ~/.config/systemd/user/
#   systemctl --user daemon-reload
set -euo pipefail

# 默认帽（2026-07-08 W0-OPS0 下调）：07-07 实测 5 次 global OOM 都在 RSS 5.7-6.4G
# 时发生——11G 主机与 Plex/游戏服共存，MemoryMax=7G 的旧帽等于全局 OOM 先于硬帽
# 触发（帽形同虚设）。降为 High=4.5G 软限流 / Max=5.5G 硬帽：宁可研究任务被自己的
# 帽杀（Result=oom-kill 可验尸），不可拖全主机陪葬。特大作业显式抬
# RESEARCH_MEMORY_* 环境变量并错峰跑。
MEMORY_HIGH="${RESEARCH_MEMORY_HIGH:-4.5G}"
MEMORY_MAX="${RESEARCH_MEMORY_MAX:-5.5G}"

usage() {
  echo "用法: $0 <tag> -- <command...>" >&2
  echo "示例: $0 ta-zoo -- .venv/bin/python -m research.evaluate --factors size" >&2
  exit 2
}

[[ $# -ge 3 ]] || usage
TAG="$1"; shift
[[ "$1" == "--" ]] || usage
shift

[[ "${TAG}" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "tag 只允许 [A-Za-z0-9._-]: ${TAG}" >&2; exit 2; }
UNIT="research-${TAG}"

if systemctl --user is-active --quiet "${UNIT}.service" 2>/dev/null; then
  echo "错误: ${UNIT} 已在运行（journalctl --user -u ${UNIT} -f 跟进度）" >&2
  exit 1
fi
# 上一轮失败残骸会挡住同名 unit 重启（失败 unit 常驻内存直到 reset-failed，
# 这正是验尸窗口——重启同名任务时才清）
systemctl --user reset-failed "${UNIT}.service" 2>/dev/null || true

# 不加 --collect：失败 unit（Result=oom-kill/exit-code/signal）保留在内存里供
# show -p Result 验尸，成功的自动卸载——--collect 会连失败的一起立即回收，
# 验尸命令反而误报 Result=success（253 systemd 257 实测）。上一轮失败残骸
# 会挡住同名 unit 重启，发射前 reset-failed 清掉（见上）。
systemd-run --user --unit="${UNIT}" \
  --working-directory="$(pwd)" \
  -p MemoryHigh="${MEMORY_HIGH}" -p MemoryMax="${MEMORY_MAX}" \
  -p OnFailure="stock-research-notify@${UNIT}.service" \
  "$@"

cat <<EOF
已发射: ${UNIT}
  跟进度:  journalctl --user -u ${UNIT} -f
  看状态:  systemctl --user status ${UNIT}
  判死因:  systemctl --user show ${UNIT} -p Result -p ExecMainStatus
           (Result=oom-kill 即内存击杀; 失败会通知 + 记 logs/failures.log)
  停任务:  systemctl --user stop ${UNIT}
EOF
