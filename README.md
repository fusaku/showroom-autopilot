# Showroom Autopilot

一个全自动的 SHOWROOM 直播监控、录制、处理和上传系统，专为 AKB48 系列偶像团体设计。

## 📋 目录

- [功能特性](#功能特性)
- [系统架构](#系统架构)
- [目录结构](#目录结构)
- [环境要求](#环境要求)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [使用方法](#使用方法)
- [工作流程](#工作流程)
- [常见问题](#常见问题)

## ✨ 功能特性

### 已实现功能

- **智能监控** - 实时监控成员直播状态，支持多实例负载均衡
- **自动录制** - 检测到直播开始时自动启动录制服务
- **视频处理** - 智能合并 TS 分段，支持跨日直播处理
- **字幕嵌入** - 自动匹配和嵌入 ASS 字幕文件
- **YouTube 上传** - 自动上传到 YouTube，支持多账号配额管理
- **负载均衡** - 智能分配录制任务到多个实例

### 核心特性

- **进程管理** - 自动检测和清理重复进程，防止资源浪费
- **故障恢复** - 智能重启异常进程，确保录制不中断
- **文件锁机制** - 防止多实例同时处理相同任务
- **配额管理** - YouTube API 配额智能切换，避免超限
- **日志记录** - 完整的日志系统，便于问题排查
- **GitHub Pages 发布** - 自动发布上传记录到静态网站

### 🚧 计划功能

- 完整的数据库初始化脚本
- Web 管理界面
- 实时监控面板
- 更多直播平台支持

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────┐
│                    Oracle 数据库                         │
│          (直播状态 + 实例管理 + 历史记录)                 │
└────────────┬────────────────────────────────┬───────────┘
             │                                │
    ┌────────┴────────┐              ┌────────┴────────┐
    │   Monitor 模块   │              │  Recorder 模块   │
    │  (直播监控)      │              │  (录制管理)      │
    ├─────────────────┤              ├─────────────────┤
    │ • 轮询直播状态   │              │ • 智能启动录制   │
    │ • 更新数据库     │              │ • 进程监控       │
    │ • 负载均衡       │              │ • 视频合并       │
    │ • 多实例支持     │              │ • 字幕处理       │
    └─────────────────┘              │ • YouTube 上传   │
                                     └─────────────────┘
```

**核心组件说明:**

- **Monitor (监控器)**: 实时检测成员直播状态，支持多实例分片监控
- **Recorder (录制器)**: 管理录制进程，处理视频文件，上传到 YouTube
- **Load Balancer (负载均衡)**: 智能分配录制任务到不同实例
- **Database (数据库)**: 存储直播状态、实例信息和历史记录

## 📁 目录结构

```
showroom-autopilot-master/
├── data/                          # 成员配置数据
│   ├── AKB48_members.yaml        # AKB48 成员配置
│   ├── SKE48_members.yaml        # SKE48 成员配置
│   ├── NMB48_members.yaml        # NMB48 成员配置
│   ├── HKT48_members.yaml        # HKT48 成员配置
│   ├── NGT48_members.yaml        # NGT48 成员配置
│   └── STU48_members.yaml        # STU48 成员配置
│
├── monitor/                       # 监控模块
│   ├── monitor_showroom.py       # 主监控脚本
│   ├── manage_instances.py       # 实例管理
│   ├── load_balancer_module.py   # 负载均衡器
│   ├── config.py                 # 配置文件
│   ├── logger_config.py          # 日志配置
│   └── db_members_loader.py      # 成员数据加载器
│
├── recorder/                      # 录制模块
│   ├── showroom-smart-start.py   # 智能启动服务
│   ├── checker.py                # 文件检查器
│   ├── merger.py                 # 视频合并器
│   ├── subtitle_processor.py     # 字幕处理器
│   ├── upload_youtube.py         # YouTube 上传器
│   ├── github_pages_publisher.py # GitHub Pages 发布器
│   ├── restart_handler.py        # 重启处理器
│   ├── config.py                 # 配置文件
│   ├── logger_config.py          # 日志配置
│   └── db_members_loader.py      # 成员数据加载器
│
└── shared/                        # 共享模块
    ├── config.py                 # 全局配置
    ├── logger_config.py          # 日志配置
    ├── db_members_loader.py      # 成员数据加载器
    ├── db_credentials.key        # 数据库凭证（需配置）
    └── credentials/              # API 凭证目录
        ├── autoupsr/             # 主 YouTube 账号
        ├── 48g-SR/               # 副 YouTube 账号
        └── idol-SR/              # 第三 YouTube 账号

```

## 💻 环境要求

### 系统要求

- **操作系统**: Ubuntu 24.04 LTS（推荐）或其他 Linux 发行版
- **Python**: 3.8+
- **数据库**: Oracle Database（需要钱包文件）
- **存储空间**: 至少 100GB（用于视频文件）

### 必需软件

```bash
# FFmpeg（视频处理）
ffmpeg >= 4.3

# Python 包
cx_Oracle >= 8.3.0
httpx >= 0.24.0
psutil >= 5.9.0
google-api-python-client >= 2.0.0
google-auth-oauthlib >= 0.4.0
PyYAML >= 6.0
```

### 可选软件

- Git（用于 GitHub Pages 发布）
- YouTube API 密钥（用于自动上传）

## 🚀 快速开始

### 1. 环境准备

```bash
# 克隆仓库
git clone <repository-url>
cd showroom-autopilot-master

# 安装 Python 依赖
pip install cx_Oracle httpx psutil PyYAML
pip install google-api-python-client google-auth-oauthlib

# 安装 FFmpeg (Ubuntu)
sudo apt update
sudo apt install ffmpeg
```

### 2. 数据库配置

创建 `shared/db_credentials.key` 文件 (第一行用户名,第二行密码):

```
your_username
your_password
```

编辑 `shared/config.py` 设置数据库钱包路径:

```python
WALLET_DIR = "/path/to/your/Wallet_SRDB"
TNS_ALIAS = "srdb_high"
```

### 3. 配置成员信息

编辑 `data/` 目录下的 YAML 文件，启用要监控的成员:

```yaml
members:
  - id: member_id
    room_id: '12345'
    name_jp: 成员名
    enabled: true  # 设置为 true 启用监控
    # ... 其他配置
```

### 4. YouTube 配置 (可选)

如需自动上传功能:

1. 在 Google Cloud Console 创建项目并启用 YouTube Data API v3
2. 下载 OAuth 2.0 凭证文件
3. 放置到 `shared/credentials/autoupsr/client_secret.json`
4. 首次运行会提示授权

### 5. 启动服务

```bash
# 启动监控服务
INSTANCE_ID=monitor-a python monitor/monitor_showroom.py

# 启动录制服务 (另一个终端)
INSTANCE_ID=recorder-a python recorder/showroom-smart-start.py
```

## 📖 使用方法

### 监控服务

**单实例模式** (监控所有成员):
```bash
INSTANCE_ID=monitor-a python monitor/monitor_showroom.py
```

**多实例模式** (自动分片):
```bash
# 实例 A
INSTANCE_ID=monitor-a python monitor/monitor_showroom.py

# 实例 B (在另一台机器或终端)
INSTANCE_ID=monitor-b python monitor/monitor_showroom.py
```

系统会自动从数据库读取活跃实例数,平均分配监控任务。

### 录制服务

```bash
# 启动智能录制管理器
INSTANCE_ID=recorder-a python recorder/showroom-smart-start.py
```

录制器会:
1. 监控数据库中分配的直播任务
2. 自动启动 Showroom Live Watcher 录制进程
3. 检测并清理重复进程
4. 异常时自动重启

### 实例管理

```bash
# 查看所有实例状态
python monitor/manage_instances.py status

# 注册新实例
python monitor/manage_instances.py register monitor-c

# 停用实例
python monitor/manage_instances.py deactivate monitor-c
```

### 手动操作

```bash
# 手动合并视频
python recorder/merger.py

# 手动上传到 YouTube
python recorder/upload_youtube.py
```

## ⚙️ 配置说明

### 主配置文件 (`shared/config.py`)

```python
# 数据库配置
WALLET_DIR = "/home/ubuntu/Wallet_SRDB"  # Oracle 钱包目录
TNS_ALIAS = "srdb_high"                   # TNS 别名

# 路径配置
PARENT_DIR = Path("~/Downloads/Showroom/active")  # 录制文件目录
OUTPUT_DIR = Path("/mnt/video/merged")             # 输出目录

# 监控配置
CHECK_INTERVAL = 30                      # 检测间隔(秒)
LIVE_INACTIVE_THRESHOLD = 60            # 直播结束判定时间(秒)
MAX_WORKERS = 16                         # 并发线程数

# 视频处理
SUBTITLE_OFFSET_SECONDS = 12            # 字幕偏移(秒)
FFMPEG_LOGLEVEL = "error"               # FFmpeg 日志级别
```

### 成员配置 (`data/*.yaml`)

每个成员的配置包含:

- `id`: 成员唯一标识
- `room_id`: SHOWROOM 房间号
- `name_jp` / `name_en`: 日文/英文名
- `team`: 所属团队
- `enabled`: 是否启用监控
- `youtube`: YouTube 上传配置 (标题、描述、标签等)

### 实例配置

通过环境变量 `INSTANCE_ID` 指定实例标识:

- 监控实例: `monitor-a`, `monitor-b`, ...
- 录制实例: `recorder-a`, `recorder-b`, ...

系统会自动从数据库读取实例数量并分配任务。

## 🗄️ 数据库要求

系统需要 Oracle 数据库支持,包含以下表结构:

### 必需表

- **LIVE_STATUS** - 存储实时直播状态
- **INSTANCES** - 管理监控和录制实例
- **SHOWROOM_LIVE_HISTORY** - 保存历史直播记录

### 配置数据库

1. 准备 Oracle 数据库实例
2. 下载并配置 Wallet 文件
3. 创建所需的表结构
4. 在 `shared/config.py` 中配置连接信息

> **注意**: 数据库初始化 SQL 脚本将在后续版本中提供。

## 🔄 工作流程

### 完整工作流

```
监控服务 → 检测直播开始 → 更新数据库 → 负载均衡分配
                                        ↓
                                   录制服务启动
                                        ↓
                            Showroom Live Watcher 录制
                                        ↓
                              检测直播结束 + 文件稳定
                                        ↓
                                   合并 TS 文件
                                        ↓
                                   嵌入字幕 (可选)
                                        ↓
                                上传到 YouTube (可选)
                                        ↓
                              发布到 GitHub Pages (可选)
```

### 关键特性

**智能进程管理**
- 每轮只扫描 1 次系统进程 (性能提升 100 倍)
- 自动检测并清理重复进程
- 异常进程自动重启

**灵活的实例模式**
- 单实例: 一个监控器监控所有成员
- 多实例: 自动分片,每个监控器负责部分成员
- 动态扩展: 新增实例自动重新分配任务

**智能视频处理**
- 支持跨日直播合并
- 自动检测文件夹分组
- 字幕自动匹配和时间轴调整

## ❓ 常见问题

### Q: 如何添加新成员?

编辑对应团队的 YAML 文件 (如 `data/AKB48_members.yaml`),添加成员信息并设置 `enabled: true`,然后重启监控服务。

### Q: 支持多台服务器运行吗?

支持。只需在每台服务器上设置不同的 `INSTANCE_ID`,系统会自动分配任务。例如:
- 服务器 A: `INSTANCE_ID=monitor-a`
- 服务器 B: `INSTANCE_ID=monitor-b`

### Q: 录制失败怎么办?

检查:
1. Showroom Live Watcher 是否安装且版本最新
2. 网络连接是否正常
3. 磁盘空间是否充足
4. 查看日志文件排查错误

### Q: YouTube 上传失败?

可能原因:
- API 配额不足 (系统会自动切换备用账号)
- 视频文件损坏 (检查 FFmpeg 合并日志)
- 网络问题 (系统会自动重试)

### Q: 如何查看运行日志?

日志默认保存在 `~/logs/` 目录:
```bash
tail -f ~/logs/monitor.log    # 监控日志
tail -f ~/logs/recorder.log   # 录制日志
```

### Q: 系统需要什么权限?

- 读写录制目录和输出目录的权限
- 创建和终止进程的权限
- 访问数据库的权限
- (可选) YouTube API 访问权限

## 🔧 技术栈

- **Python 3.8+** - 主要开发语言
- **Oracle Database** - 数据存储
- **Showroom Live Watcher** - 直播录制
- **FFmpeg** - 视频处理
- **YouTube Data API v3** - 视频上传
- **httpx** - HTTP 请求
- **psutil** - 进程管理

## 📝 更新日志

### v2.0 (当前版本)

- ✨ 多实例负载均衡
- 🚀 智能进程管理 (性能提升 100 倍)
- 🔧 自动清理重复进程
- 📊 改进的数据库设计
- 🐛 修复跨日直播处理

### 未来计划

- 🚧 完整的数据库初始化脚本
- 🚧 Web 管理界面
- 🚧 实时监控面板
- 🚧 Docker 容器化部署

## 📄 许可证

本项目采用 MIT 许可证。

## 🙏 致谢

- [Showroom Live Watcher](https://github.com/wlerin/showroom) - 视频下载工具
- [FFmpeg](https://ffmpeg.org/) - 音视频处理
- [SHOWROOM](https://www.showroom-live.com/) - 直播平台

---

**免责声明**: 本项目仅供学习和研究使用,请遵守相关平台的服务条款。
