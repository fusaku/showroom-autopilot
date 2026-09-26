# 08 用户提供 DDL 核对记录

日期：2026-09-19。来源：用户指定的 `/Users/ka/Code/python/DDL/`，共 15 个 txt 文件（含后续补充的两张表与两个视图）。证据级别为“提供的 DDL 确认”，未连接数据库验证当前部署是否一致。未执行任何 DDL，也未修改源文件。

## 覆盖情况

已取得 MEMBERS、GROUPS、YOUTUBE_CONFIGS、YOUTUBE_TAGS、LIVE_STATUS、SHOWROOM_LIVE_HISTORY、MEMBER_INSTANCES_HISTORY 七张业务表；另有 GLOBAL_CONFIG、INSTANCE_CONFIG、CONFIG_HISTORY 三张配置相关表。DBTOOLS$EXECUTION_HISTORY 未作为业务设计输入，未分析其内容。

文件包含 CREATE TABLE、ALTER TABLE 约束、部分索引、授权、同义词及触发器。txt 扩展名不影响作为 DDL 使用；但当前集合不是完整的可重建数据库脚本。

已补齐：ADMIN.INSTANCES、ADMIN.MEMBER_INSTANCES。

尚缺：
- 视图已补齐：ADMIN.V_INSTANCE_LOAD、ADMIN.V_MEMBER_ASSIGNMENTS，不再列为缺项。
- GROUPS_SEQ、MEMBERS_SEQ、YOUTUBE_CONFIGS_SEQ、YOUTUBE_TAGS_SEQ、GLOBAL_CONFIG_SEQ、INSTANCE_CONFIG_SEQ、CONFIG_HISTORY_SEQ、INSTANCES_SEQ、MEMBER_INSTANCES_SEQ 的 CREATE SEQUENCE 定义。
- 成员分配历史 ID 生成机制：已确认分配表触发器自动插入历史，但历史 ID 的默认/触发器生成定义仍未提供。

## 已确认约束及生成规则

| 对象 | 提供的 DDL 确认结果 |
|---|---|
| GROUPS | ID 主键；NAME 唯一且非空；插入时空 ID 由 GROUPS_SEQ 填充；更新触发器刷新 UPDATED_AT |
| MEMBERS | ID 主键；MEMBER_ID 唯一；GROUP_ID、ROOM_ID、NAME_JP、NAME_EN、MEMBER_ID 非空；ID 通过 MEMBERS_SEQ 触发器生成；UPDATED_AT 更新触发器 |
| YOUTUBE_CONFIGS | ID 主键；MEMBER_ID 非空且唯一，引用 MEMBERS.ID；ID 由对应序列触发器生成 |
| YOUTUBE_TAGS | ID 主键；MEMBER_ID、TAG 非空；MEMBER_ID 引用 MEMBERS.ID；ID 由对应序列触发器生成 |
| LIVE_STATUS | ID 为 GENERATED ALWAYS AS IDENTITY 主键；MEMBER_ID、ROOM_ID、IS_LIVE、CHECK_TIME 非空；CHECK_TIME 索引 |
| SHOWROOM_LIVE_HISTORY | ID 为 GENERATED ALWAYS AS IDENTITY 主键；MEMBER_ID、ROOM_ID、STARTED_AT 非空；成员/开播时间及日期表达式索引 |
| MEMBER_INSTANCES_HISTORY | ID 主键；MEMBER_ID、INSTANCE_ID、INSTANCE_TYPE、ACTION 非空；ACTION 限定五种操作；未在文件内发现 ID 自动生成定义 |
| GLOBAL_CONFIG | CONFIG_KEY 非空且唯一；ID 通过序列触发器生成；更新时刷新 UPDATED_AT |
| INSTANCE_CONFIG | INSTANCE_ID、CONFIG_KEY 非空且组合唯一；ID 通过序列触发器生成；更新时刷新 UPDATED_AT |
| CONFIG_HISTORY | CONFIG_TYPE、CONFIG_KEY 非空；ID 通过序列触发器生成；此触发器只生成 ID，不证明配置变更会自动记录历史 |

## 对后台设计的具体影响

### 成员新增

MEMBERS 的主要字段类型：ID/GROUP_ID 为 NUMBER(10,0)；MEMBER_ID 为 VARCHAR2(150 BYTE)；ROOM_ID 为 VARCHAR2(50 BYTE)；NAME_EN 为 VARCHAR2(100 BYTE)；NAME_JP/TEAM 为 NVARCHAR2(100)；ROOM_URL_KEY 为 VARCHAR2(200 BYTE)。ENABLED 为 NUMBER(1,0)，默认 1，但未在提供文件中设置 NOT NULL 或仅允许 0/1 的 CHECK。

新增 API 应明确校验并写入 enabled，不能以数据库默认值替代全部业务校验。有效的插入触发器允许省略成员 ID，但其序列定义尚缺，不能直接认定整套导出可恢复运行。

MEMBERS.MEMBER_ID 可达 150 字节，而 LIVE_STATUS 与 SHOWROOM_LIVE_HISTORY 的 MEMBER_ID 仅 50 字节。为兼容现有处理链，后台规划应将新业务 ID 限制为不超过 50 字节，并按实际数据库字符集验证。不要直接放开成员表自身的 150 字节上限，也不在本次调整现有字段。

### 删除与关联

MEMBERS.GROUP_ID → GROUPS.ID 使用 ON DELETE CASCADE；YOUTUBE_CONFIGS/YOUTUBE_TAGS → MEMBERS.ID 同样使用级联删除。因此物理删除组合会波及成员及其上传配置/标签。初版后台继续采用停用/归档，不开放物理删除组合或成员。

LIVE_STATUS、SHOWROOM_LIVE_HISTORY 文件内未声明成员外键；MEMBER_INSTANCES_HISTORY 文件内也未声明成员外键。不能因此断言删除成员会清除所有历史，补充的 MEMBER_INSTANCES.MEMBER_ID 外键同样使用 ON DELETE CASCADE。

YOUTUBE_CONFIGS.MEMBER_ID 的唯一约束确认每成员最多一条上传配置；USE_PRIMARY_ACCOUNT 默认 1，但仍是数值标志，不能据此设计三个账号任意选择的数据库接口。

### 时间、唯一性与名称解析

这些时间列使用 TIMESTAMP(6)，没有时区声明；DEFAULT CURRENT_TIMESTAMP 不等于列保存了时区。仍须确认数据库会话与两台主机时间约定。

LIVE_STATUS 文件仅展示 ID 主键及 CHECK_TIME 普通索引，未展示 MEMBER_ID 唯一约束。监控 MERGE 使用 MEMBER_ID 匹配；后台不要假定数据库已保证每成员只有一行，也不要自行在生产新增唯一约束。

LIVE_STATUS、SHOWROOM_LIVE_HISTORY 的 owner 已确认为导出中的 ADMIN，但两个私有同义词语句被注释，不能将其视为已创建。原程序未限定 owner 的表名能否解析仍依赖实际登录身份和同义词配置。

### 配置表与权限

GLOBAL_CONFIG、INSTANCE_CONFIG、CONFIG_HISTORY 确实存在于导出，但当前 Python 源码搜索未找到这三者的引用。不能把它们直接用作已接通的热配置或自动审计机制。

多张成员/配置表向现有应用主体授予了增删改查权限。新后台第一阶段应单独配置只读权限，不能把现有账号描述为只读。本文不复制任何密码或连接凭据。

## 后续动作

视图 DDL 已收到；补齐九个已引用序列及分配历史 ID 生成机制后，再完善可重建性核对。已有成员数据不必导出；本阶段继续只处理结构，不执行建表/改表操作。


## 两张补充表的核对结果

- INSTANCES：ID 主键、INSTANCE_ID 非空并有唯一索引；实例类型为 monitor/recorder，状态 CHECK 为 active/inactive/maintenance。ID 由 INSTANCES_SEQ 插入触发器生成，更新触发器刷新 UPDATED_AT。CONFIG_VERSION 的“变更自增”只是列注释，此次提供的更新触发器并未实现它；CURRENT_LOAD、LAST_HEARTBEAT 的实际维护仍需核实。
- MEMBER_INSTANCES：ID 主键；MEMBER_ID、INSTANCE_ID、INSTANCE_TYPE 非空；三者组合唯一；MEMBER_ID 引用 MEMBERS.ID 并级联删除。未在提供文件中发现 INSTANCE_ID 指向 INSTANCES 的外键，也不能把三列唯一解释成一个成员只能分配到一台录制器。
- 分配历史：MEMBER_INSTANCES_HISTORY_TRG 在分配插入、删除、启停和实例迁移时记录历史；其他更新直接返回。OPERATED_BY 来自数据库会话用户，不是网页登录用户。历史 INSERT 未包含 ID，而已有历史表 DDL 没有 ID 默认生成定义；这是导出材料待补齐项，不能据此断言正在运行的数据库缺少生成机制。

## 两个视图已收到

已提供 ADMIN.V_INSTANCE_LOAD 与 ADMIN.V_MEMBER_ASSIGNMENTS 的 CREATE VIEW 定义、注释、SELECT 授权和公共同义词。视图存在性不再是导出资料缺项；当前生产对象的有效性未在线验证。

V_INSTANCE_LOAD 按实例 ID/类型汇总 ENABLED=1 的分配行，计算 CURRENT_LOAD、AVAILABLE_CAPACITY、LOAD_PERCENT，并显示实例心跳。这里的负载是分配数量，不是实时录像进程数，也不是 CPU 负载。

V_MEMBER_ASSIGNMENTS 联查成员、组合和分配，以 MAX 汇总 monitor/recorder 实例及启用状态，提供 STATUS_DESC。一个成员若有多个同类型实例，实例 ID 与启用值可能分别来自不同记录，不应作为唯一完整分配明细。

## DDL 分类归档

分类副本在 `/Users/ka/Code/python/DDL/organized/`。15 份原始 txt 保留不动，副本改用 sql 扩展名，逐字节和 SHA-256 验证一致。按成员、上传、直播、实例、配置、视图和工具对象分类，附属触发器/索引/授权仍在原对象文件内，附属对象索引和依赖说明单独提供。没有执行 SQL。
