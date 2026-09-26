# 05 配置与状态设计书

## 配置来源及生效

| 来源 | 当前作用 | 生效边界 |
|---|---|---|
| shared/config.py | 路径、周期、同步、上传发布参数 | Python 模块加载时求值，不能承诺自动热更新 |
| 环境变量/命令行 | monitor/recorder 实例与成员选择 | 启动入口解释，按脚本逐个确认 |
| Oracle 成员及上传配置 | 构造 ENABLED_MEMBERS | config 在模块加载时保存快照 |
| db_members_loader 缓存 | 60 秒缓存及显式刷新函数 | 有刷新函数不等于所有调用者都使用它 |
| 独立 YAML 管理工具 | 更新播放列表等配置 | 单独运行，数据库提交不等于进程立即读取 |

**关键事实**：`shared/config.py:get_enabled_members()` 返回 ENABLED_MEMBERS 快照；smart-start 的循环刷新代码被注释。页面不能把“数据库已保存”展示为“所有运行进程已生效”。即使重启，还要核实外部 showroom.py 是否需要自己的成员配置。

现有启停字段被不同查询读取：录制状态 JOIN 会查询 m.ENABLED，但录制管理的 monitored_members 又来自启动时列表。因此添加、停用、重新启用应逐项验证，不能给出统一的立即生效承诺。

后台第一版显示“已保存；运行端生效状态未确认”，不自动重启录制服务。只有取得运行端版本/配置确认信息，才可显示“已生效”。

## 本地配置快照

以下是工作区值，不代表生产值，也不代表实际耗时：

| 项目 | 当前值 |
|---|---|
| REQUEST_INTERVAL | 5 秒 |
| RESTART_CHECK_INTERVAL | 3 秒 |
| CHECK_INTERVAL | 30 秒 |
| FILE_STABLE_TIME | 5 秒 |
| MAX_WORKERS | 16 |
| SYNC_MODE | main |
| MAIN_MEMBER_ID | hashimoto_haruna |
| YOUTUBE_DELETE_AFTER_UPLOAD | False |

部分名称容易误读：FileLock 接受 timeout 参数，但当前实现使用非阻塞 flock，没有实现按 timeout 等待。文档不把配置值描述成已实现保证。

## 原始证据与页面语义（规划）

| 证据 | 可以展示 | 不能据此断言 |
|---|---|---|
| IS_LIVE + CHECK_TIME | 最新检测结果及时间 | 正在成功录制 |
| 实例分配记录 | 计划由哪个实例负责 | 实例在线、进程已启动 |
| LAST_HEARTBEAT | 最后上报时间 | 心跳永远定期更新；需核实更新来源 |
| 进程 PID/启动时间 | 观察到录制进程 | 文件正常增长、媒体有效 |
| TS 新增/大小变化 | 观察窗口内有数据写入 | 全程无丢片 |
| filelist.txt | 已生成检查/合并清单 | 清单非空或合并完成 |
| chunk_*.mp4 | 存在加工片段 | 完整场次加工完成 |
| .merged | 来源目录有合并标记 | 产物当前存在且完整 |
| .mp4.uploaded | 已保存上传结果标记/video_id | 远端最终处理成功 |
| videos.json/上传信息 | 发布记录或本地记录 | 远端链接现在一定可访问 |

清理可能删除标记；标记消失不应自动把历史成功状态变成失败。需要长时间历史的后台，应保存自己的观察记录，并记录来源与时间。

## 状态模型

使用多维字段而非单一“正常/异常”：

- `live_state`：live / offline / unknown。
- `recording_state`：observed_writing / process_only / not_observed / unknown。
- `processing_stage`：检查、同步、加工、合并、上传、发布、未知；允许同一场次多阶段重叠。
- `freshness`：fresh / stale / unavailable。
- `observed_at`、`source`、`reason`：每个状态附证据时间与来源。

“未观察到进程”只有在采集权限、主机连接和成员映射均正常时才有意义；采集失败显示未知。阈值在试运行后确定，以实际采样周期为依据；不直接复制录制器的重启阈值触发后台告警。

## 场次关联

优先使用业务 member_id 与 started_at，并保留源目录、环境和原始记录 ID。若时间来源不一致或同一直播有多目录，保留候选关联/未关联状态，不跨场次合并。DB 时间时区和目录命名规则需现场确认，页面不得直接给无时区时间补 UTC。

源码依据：[配置](../../shared/config.py)、[成员缓存](../../shared/db_members_loader.py)、[录制管理](../../recorder/showroom-smart-start.py)、[清理](../../recorder/cleanup.py)、[上传标记](../../recorder/upload_youtube.py)。
