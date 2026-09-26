# 04 部署与运维设计书

## 现状与部署清单

两边完整同步项目为用户确认。`deploy/` 为空，仓库没有可供核对的 systemd unit。以下为代码支持的角色，不能直接当作两台服务器已启用服务表。

| 环境/角色 | 预期入口 | 需要核实 |
|---|---|---|
| 检测角色，主机待确认 | monitor/monitor_showroom.py | INSTANCE_ID、分片与成员参数、启动账号 |
| 3C 录制管理 | recorder/showroom-smart-start.py | 对应实例、外部抓流路径、进程所有者 |
| 特定成员服务恢复 | recorder/restart_handler.py | 是否启用、成员、对应 showroom-成员.service |
| 3C 校验 | recorder/checker.py | TS/字幕目录、同步模式 |
| 4C 加工 | recorder/checker_4c.py | incoming/processed/merged、资源限制 |
| 公共上传发布 | recorder/upload_youtube.py 等 | 自动触发或独立调度、账号、发布目录 |

源码中的 `merger.upload_if_needed()` 有上传子进程启动路径。不能在未检查现有调度前再加一份上传常驻服务。

## 路径与依赖

当前配置示例路径：源文件 `~/Downloads/Showroom/active`，输出 `/mnt/video/merged`，4C incoming `/mnt/video/data/incoming_ts`，processed `/mnt/video/data/processed_ts`。波浪号随运行账号变化；服务器不能直接照搬本地路径。

Python 外部依赖涉及 cx_Oracle、httpx、psutil、Google API/OAuth 库、PyYAML、tabulate、requests、OCI SDK 等，按启用模块核对。系统依赖涉及 Oracle 客户端/Wallet、FFmpeg/FFprobe、rsync/SSH、Git、Linux 服务管理。当前未找到统一依赖锁定文件，不能保证在全新环境一次安装完成。

monitor/recorder 下的 config、db_members_loader、logger_config 为指向 shared 的软链接。同步方式应保留有效链接或等效文件内容；必须核对目标机链接是否仍可解析。

## 本次重构兼容清单

共同发布：

- 修改 `recorder/merger.py`。
- 修改 `recorder/upload_youtube.py`。
- 新增 `recorder/file_lock.py`。

缺少新增模块将导致 checker/checker_4c 的导入链失败。它在文档基线工作区仍是未跟踪文件；通过 Git 发布前须确认纳入提交。完整同步代码不代表自动带上 Git 未提交文件。

备份两台机器现有代码及环境配置，确认三文件到位后再安排加载新版本；不要在复制一半时触发任务启动。若回退，恢复匹配的旧 merger 与旧 upload_youtube；无需为回退触碰业务数据、token 或录像文件。

长驻进程可能继续使用已加载模块，后续创建的子进程可能读取新磁盘文件，所以“没有重启”不保证所有任务仍使用旧代码。具体切换窗口应依据服务器现有运行方式决定。

## 新后台部署原则（规划）

独立仓库、目录、Python 环境、服务身份、配置和日志；不导入原项目 config 初始化逻辑，不启动或重启原项目服务。其故障不应影响原系统控制流程。

后台可先在本地开发，之后部署独立主机或现有主机的受限服务。共机部署仍会共享 CPU、内存、磁盘和数据库连接，需设连接池上限、查询超时、缓存和限频。具体限额通过测试确定，不以浏览器每次刷新直接触发全盘扫描。

访问方式尚未最终确定；用户已讨论服务器网页方案。远程访问设计需要认证、传输保护和最小数据库权限；本设计不提供公网开放或防火墙修改命令。

## 运维观察与保留

记录版本、启动时间、采集时间和环境标识。检查日志增长和视频磁盘余量；后台采集日志应设截断与保留期。凭据轮换、数据库备份、媒体备份由现有运维方案处理，当前仓库不足以证明备份覆盖和恢复可用。

源码依据：[目录配置](../../shared/config.py)、[启动管理](../../recorder/showroom-smart-start.py)、[服务恢复](../../recorder/restart_handler.py)、[合并触发](../../recorder/merger.py)。
