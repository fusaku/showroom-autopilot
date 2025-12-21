# Showroom 直播录制自动化处理系统

一个完整的 Showroom 直播录制自动化处理系统，支持直播状态监控、录制服务自动重启、文件完整性检查、智能合并、YouTube 自动上传和 GitHub Pages 自动发布。

## 🌟 系统特性

### 核心功能模块

#### 1. 直播状态监控 (`monitor_showroom.py`)
- **多IP轮询机制**: 使用10个出站IP分散负载，避免API限流
- **数据库集成**: 实时记录直播状态到 Oracle 数据库
- **智能周期调度**: 动态计算最优检测周期，确保快速发现直播
- **错误恢复**: 数据库连接断开自动重连，队列堆积预警

#### 2. 录制服务管理
- **智能重启** (`restart_handler.py`): 检测录制卡死并自动重启 systemd 服务
- **跨日直播支持**: 正确处理跨越午夜的长时间直播
- **宽限期机制**: 避免在直播刚开始时误判
- **多成员并发** (`showroom-smart-start.py`): 统一管理多个成员的录制进程

#### 3. 文件完整性检查 (`checker.py`)
- **流完整性验证**: FFprobe 检测每个 TS 文件的音视频流
- **增量检查**: 只检查新生成的稳定文件，提高效率
- **智能分组**: 自动识别跨日文件夹，将同一场直播的多个文件夹分组
- **字幕匹配**: 智能查找并自动创建字幕文件软链接

#### 4. 智能合并 (`merger.py`)
- **文件锁保护**: 防止多进程重复合并同一文件
- **跨文件夹合并**: 自动合并属于同一场直播的多个文件夹
- **异步上传触发**: 合并完成后自动触发独立上传进程

#### 5. YouTube 自动上传 (`upload_youtube.py`)
- **双账号支持**: 主账号用于特定成员，副账号用于其他成员
- **成员配置**: 从 `members.json` 自动加载每个成员的上传配置
- **断点续传**: 支持大文件分块上传，失败自动重试
- **配额管理**: 智能检测配额耗尽，等待重置后继续
- **超时保护**: HTTP 请求超时自动重试，防止卡死

#### 6. GitHub Pages 发布 (`github_pages_publisher.py`)
- **视频索引**: 自动更新 `videos.jsonl` 文件
- **字幕处理**: 复制字幕文件并应用时间轴偏移
- **Git 自动化**: 自动提交并推送到 GitHub Pages 仓库

## 📋 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                     直播状态监控层                             │
│  ┌──────────────────┐        ┌──────────────────┐           │
│  │ monitor_showroom │───────▶│  Oracle 数据库    │           │
│  │  (10 IP 轮询)    │        │  (直播状态表)      │           │
│  └──────────────────┘        └──────────────────┘           │
│           │                           │                     │
│           ▼                           ▼                     │
│  ┌──────────────────┐        ┌──────────────────┐           │
│  │ restart_handler  │        │ smart-start      │           │
│  │  (单成员重启)    │         │  (多成员管理)      │           │
│  └──────────────────┘        └──────────────────┘           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     文件处理层                                │
│  ┌──────────────────┐        ┌──────────────────┐           │
│  │    checker.py    │───────▶│    merger.py     │           │
│  │  (文件完整性检查) │         │  (智能合并)        │           │
│  └──────────────────┘        └──────────────────┘           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     发布层                                   │
│  ┌──────────────────┐        ┌──────────────────┐           │
│  │ upload_youtube   │───────▶│ github_publisher │           │
│  │  (YouTube 上传)  │         │  (GitHub Pages)  │           │
│  └──────────────────┘        └──────────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 快速开始

### 环境要求

- **操作系统**: Ubuntu 20.04+ / CentOS 7+
- **Python**: 3.8+
- **外部工具**: FFmpeg, FFprobe, Git
- **数据库**: Oracle Database (可选，用于状态监控)

### 安装步骤

1. **安装系统依赖**
```bash
sudo apt update
sudo apt install ffmpeg python3 python3-pip git python3-venv
```

2. **创建虚拟环境**
```bash
python3 -m venv ~/venv
source ~/venv/bin/activate
```

3. **安装 Python 依赖**
```bash
pip install google-auth google-auth-oauthlib google-api-python-client
pip install cx_Oracle requests requests-toolbelt
```

4. **配置 Oracle 客户端** (如果使用状态监控)
```bash
# 下载 Oracle Instant Client
# 解压到 ~/oracle-client
# 设置环境变量
export LD_LIBRARY_PATH=~/oracle-client:$LD_LIBRARY_PATH
```

### 配置文件

#### 1. 主配置 (`config.py`)

```python
# 路径配置
PARENT_DIR = Path("~/Downloads/Showroom/active").expanduser()
OUTPUT_DIR = Path("~/Videos/merged").expanduser()

# 数据库配置 (可选)
DB_USER = "your_username"
DB_PASSWORD = "your_password"
TNS_ALIAS = "your_db_alias"

# YouTube 配置
ENABLE_AUTO_UPLOAD = True
YOUTUBE_CLIENT_SECRET_PATH = BASE_DIR / "credentials" / "autoupsr" / "client_secret.json"
YOUTUBE_TOKEN_PATH = BASE_DIR / "credentials" / "autoupsr" / "youtube_token.pickle"

# GitHub Pages 配置
ENABLE_GIT_AUTO_PUBLISH = True
GITHUB_PAGES_REPO_PATH = Path("~/your-username.github.io").expanduser()
```

#### 2. 成员配置 (`members.json`)

```json
{
  "members": [
    {
      "id": "hashimoto_haruna",
      "room_id": "61570",
      "name_jp": "橋本 陽菜",
      "name_en": "Hashimoto Haruna",
      "team": "AKB48 チーム8",
      "enabled": true,
      "room_url_key": "48_Haruna_Hashimoto",
      "youtube": {
        "title_template": "",
        "description_template": "...",
        "tags": ["AKB48", "Team8", "橋本陽菜"],
        "category_id": "22",
        "privacy_status": "public",
        "playlist_id": "",
        "use_primary_account": true
      }
    }
  ]
}
```

#### 3. 数据库凭证 (`db_credentials.key`)

```
username
password
```

### YouTube API 设置

1. **创建 Google Cloud 项目**
   - 访问 https://console.cloud.google.com
   - 创建新项目
   - 启用 YouTube Data API v3

2. **创建 OAuth 2.0 凭证**
   - 创建桌面应用凭证
   - 下载 `client_secret.json`
   - 放置到 `credentials/autoupsr/` 目录

3. **首次认证**
```bash
python upload_youtube.py
# 会自动打开浏览器进行授权
```

## 📦 部署方案

### systemd 服务配置

#### 1. 状态监控服务

创建 `/etc/systemd/system/showroom-monitor.service`:

```ini
[Unit]
Description=Showroom Live Monitor (All Members)
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/State-detection
Environment="MEMBER_ID=ALL"
ExecStart=/home/ubuntu/venv/bin/python3 /home/ubuntu/State-detection/monitor_showroom.py
Restart=always
RestartSec=60
StandardOutput=append:/home/ubuntu/logs/monitor-all.log
StandardError=append:/home/ubuntu/logs/monitor-all.log

[Install]
WantedBy=multi-user.target
```

#### 2. 智能重启服务 (单成员)

创建 `/etc/systemd/system/showroom-restart@.service`:

```ini
[Unit]
Description=Showroom Restart Handler for %i
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/State-detection
Environment="MEMBER_ID=%i"
ExecStart=/home/ubuntu/venv/bin/python3 /home/ubuntu/State-detection/restart_handler.py
Restart=always
RestartSec=30
StandardOutput=append:/home/ubuntu/logs/restart-%i.log
StandardError=append:/home/ubuntu/logs/restart-%i.log

[Install]
WantedBy=multi-user.target
```

#### 3. 多成员管理服务

创建 `/etc/systemd/system/showroom-smart-start.service`:

```ini
[Unit]
Description=Showroom Smart Start Handler (All Members)
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/State-detection
ExecStart=/home/ubuntu/venv/bin/python3 /home/ubuntu/State-detection/showroom-smart-start.py
Restart=always
RestartSec=60
StandardOutput=append:/home/ubuntu/logs/smart-start.log
StandardError=append:/home/ubuntu/logs/smart-start.log

[Install]
WantedBy=multi-user.target
```

#### 4. 文件处理服务

创建 `/etc/systemd/system/live-merge-up.service`:

```ini
[Unit]
Description=Live Merge and Upload Service
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/live-merge-up
ExecStart=/home/ubuntu/venv/bin/python3 /home/ubuntu/live-merge-up/main.py
Restart=always
RestartSec=30
StandardOutput=append:/home/ubuntu/logs/live-merge-up.log
StandardError=append:/home/ubuntu/logs/live-merge-up.log

[Install]
WantedBy=multi-user.target
```

### 启动所有服务

```bash
# 重新加载 systemd 配置
sudo systemctl daemon-reload

# 启用并启动服务
sudo systemctl enable --now showroom-monitor
sudo systemctl enable --now showroom-smart-start
sudo systemctl enable --now live-merge-up

# 启动单成员重启服务 (示例)
sudo systemctl enable --now showroom-restart@hashimoto_haruna

# 查看服务状态
sudo systemctl status showroom-monitor
sudo systemctl status showroom-smart-start
sudo systemctl status live-merge-up
```

## 🔧 工作流程详解

### 1. 直播状态监控流程

```
1. monitor_showroom.py 启动，分配 46 个成员到 10 个 IP
2. 每个 IP 负责约 5 个成员，错开 3 秒启动
3. 每 30 秒检测一次所有成员的直播状态
4. 发现直播开始 → 写入数据库 (IS_LIVE=1, STARTED_AT=时间戳)
5. 发现直播结束 → 更新数据库 (IS_LIVE=0, ENDED_AT=时间戳)
6. 数据库写入线程独立运行，使用队列缓冲
```

### 2. 录制服务管理流程

**方案 A: 单成员独立重启** (`restart_handler.py`)
```
1. 读取数据库中指定成员的直播状态
2. 如果 IS_LIVE=1，检查录制文件夹
3. 如果最新 .ts 文件超过 60 秒未更新 → 重启服务
4. 宽限期 6 秒：直播刚开始时不检查
5. 最小重启间隔 30 秒：避免频繁重启
```

**方案 B: 多成员统一管理** (`showroom-smart-start.py`)
```
1. 监控所有成员 (除 hashimoto_haruna)
2. 发现直播开始 → 启动录制子进程
3. 录制进程启动 35 秒后开始检查文件活跃度
4. 文件超过 60 秒未更新 → 重启录制进程
5. 直播结束后等待 5 分钟再终止进程
6. 每秒检查一次所有成员状态
```

### 3. 文件检查流程

```
1. checker.py 扫描 ~/Downloads/Showroom/active
2. 找到所有有 .ts 文件但没有 filelist.txt 的文件夹
3. 按成员和时间戳分组 (同一场直播可能有多个文件夹)
4. 对每个文件夹：
   - 增量检查新的稳定文件 (修改时间 > 5 秒)
   - 使用 FFprobe 验证音视频流完整性
   - 记录有效文件到内存列表
5. 直播结束判断：
   - 数据库显示 IS_LIVE=0 或
   - 所有文件夹最新文件超过 60 秒未更新
6. 查找字幕文件 (支持模糊匹配和时间戳对齐)
7. 字幕检查失败 5 次后强制通过
8. 生成 filelist.txt 标记检查完成
```

### 4. 智能合并流程

```
1. merger.py 扫描 ~/Videos/merged 目录
2. 找到所有有 filelist.txt 但没有 .mp4 的文件夹
3. 按文件夹名称分组 (去除日期和时间戳)
4. 同一组的多个文件夹：
   - 创建合并的 filelist.txt
   - 使用 FFmpeg concat 协议合并
   - 输出文件名使用最早文件夹的名称
5. 单个文件夹：
   - 直接使用其 filelist.txt 合并
6. 合并成功后：
   - 为所有相关文件夹创建 .merged 标记
   - 异步启动独立上传进程
```

### 5. YouTube 上传流程

```
1. upload_youtube.py 被 merger.py 异步启动
2. 扫描 ~/Videos/merged 目录，找到所有未上传的 .mp4
3. 循环处理：
   a. 检查配额状态 (如果今天已耗尽，退出)
   b. 重新扫描目录 (发现新合并的文件)
   c. 对每个文件：
      - 从文件名匹配成员配置
      - 判断使用主账号还是副账号
      - 应用成员专属的标题、描述、标签
      - 分块上传 (128MB/块)，支持断点续传
      - 上传成功 → 创建 .uploaded 标记
      - 保存上传信息到 recent_uploads.json
   d. 如果没有待上传文件，退出循环
4. 上传失败重试机制：
   - 每块 30 秒超时
   - 超时或失败最多重试 5 次
   - 每次重试等待 60 秒
   - 重试时重新创建上传会话
```

### 6. GitHub Pages 发布流程

```
1. github_pages_publisher.py 在上传成功后自动调用
2. 读取 recent_uploads.json 获取最新上传信息
3. 对每个视频：
   a. 检查是否已在 videos.jsonl 中
   b. 查找对应的字幕文件：
      - 按日期和成员名匹配
      - 支持时间戳最接近原则
   c. 处理字幕文件：
      - 应用 10 秒时间轴偏移
      - 重命名为视频 ID
      - 复制到 GitHub Pages 仓库
   d. 更新 videos.jsonl (追加写入)
4. Git 操作：
   a. git pull (可选)
   b. git add .
   c. git commit -m "Update videos..."
   d. git push origin main
```

## 📊 监控和日志

### 日志文件位置

```
~/logs/
├── monitor-all.log           # 状态监控日志
├── restart-{member_id}.log   # 重启处理日志
├── smart-start.log           # 多成员管理日志
└── live-merge-up.log         # 文件处理和上传日志
```

### 查看实时日志

```bash
# 监控状态监控日志
tail -f ~/logs/monitor-all.log

# 监控文件处理日志
tail -f ~/logs/live-merge-up.log

# 监控特定成员的重启日志
tail -f ~/logs/restart-hashimoto_haruna.log

# 监控多成员管理日志
tail -f ~/logs/smart-start.log
```

### 关键日志标识

- `✅` - 操作成功
- `⚠️` - 警告信息
- `❌` - 错误信息
- `🚀` - 启动/重启操作
- `📊` - 状态统计
- `🔄` - 合并操作

## 🛠️ 故障排除

### 常见问题

#### 1. 数据库连接失败

```bash
# 检查数据库配置
cat ~/State-detection/db_credentials.key

# 检查 Wallet 文件
ls -la ~/Wallet_SRDB/

# 测试连接
python3 -c "import cx_Oracle; print(cx_Oracle.clientversion())"
```

#### 2. YouTube 配额耗尽

```bash
# 查看上传日志
grep "quotaExceeded" ~/logs/live-merge-up.log

# 配额会在太平洋时间午夜重置 (对应日本时间下午 4 点或 5 点)
# 系统会自动等待重置后继续
```

#### 3. FFmpeg 合并失败

```bash
# 检查 FFmpeg 版本
ffmpeg -version

# 手动测试合并
ffmpeg -f concat -safe 0 -i filelist.txt -c copy output.mp4

# 检查文件列表格式
cat filelist.txt
# 应该是: file '/absolute/path/to/file.ts'
```

#### 4. 录制服务未启动

```bash
# 检查服务状态
systemctl status showroom-smart-start

# 查看服务日志
journalctl -u showroom-smart-start -n 100

# 重启服务
sudo systemctl restart showroom-smart-start
```

#### 5. Git 推送失败

```bash
# 检查 Git 配置
cd ~/fusaku.github.io
git remote -v
git status

# 手动推送
git pull origin main
git push origin main
```

### 性能优化建议

#### 1. 减少磁盘 I/O

```python
# config.py
CHECK_INTERVAL = 30              # 降低检查频率
LIVE_CHECK_INTERVAL = 120        # 直播中检查间隔延长
FILE_STABLE_TIME = 10            # 文件稳定时间延长
```

#### 2. 优化并发处理

```python
# config.py
MAX_WORKERS = 3                  # 根据 CPU 核心数调整
MAX_CONCURRENT_FOLDERS = 5       # 限制同时处理的文件夹数
```

#### 3. 减少数据库负载

```python
# State-detection/config.py
REQUEST_INTERVAL = 1.0           # 增加请求间隔
RESTART_CHECK_INTERVAL = 2       # 延长重启检查间隔
```

## 📈 系统监控指标

### 关键性能指标

1. **直播发现延迟**: < 30 秒 (由检测周期决定)
2. **录制启动时间**: < 10 秒
3. **文件检查速度**: ~100 个文件/分钟
4. **合并速度**: 取决于文件大小，通常 < 5 分钟
5. **上传速度**: 取决于网络带宽，支持断点续传

### 资源使用

- **CPU**: 单核 < 20% (正常运行)
- **内存**: < 500MB (监控 + 检查 + 合并)
- **磁盘 I/O**: 主要在检查和合并阶段
- **网络**: 上传时占用上行带宽

## 🔐 安全建议

1. **保护敏感文件**
```bash
chmod 600 ~/State-detection/db_credentials.key
chmod 600 ~/live-merge-up/credentials/*/client_secret.json
chmod 600 ~/live-merge-up/credentials/*/*.pickle
```

2. **使用独立用户**
```bash
# 创建专用用户
sudo adduser showroom
sudo usermod -aG sudo showroom

# 以该用户运行所有服务
```

3. **限制 API 访问**
- 在 Google Cloud Console 中限制 API 密钥的使用范围
- 定期轮换 OAuth 令牌
- 监控 API 配额使用情况

## 📝 开发计划

### 未来功能

- [ ] Web 管理界面
- [ ] 实时状态仪表板
- [ ] 邮件/Webhook 通知
- [ ] 多语言字幕支持
- [ ] 自动生成缩略图
- [ ] B站/其他平台上传支持

### 已知限制

- 依赖 Oracle 数据库 (可考虑支持 PostgreSQL/MySQL)
- YouTube API 配额限制 (每日 10,000 单位)
- 单机部署，无法水平扩展
- 字幕文件需要外部生成

## 📄 许可证

MIT License - 详见 LICENSE 文件

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

### 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 📧 联系方式

如有问题或建议，请提交 Issue 或通过邮件联系。

---

**注意**: 本项目仅供学习和个人使用。使用时请遵守相关平台的服务条款和版权法律。
