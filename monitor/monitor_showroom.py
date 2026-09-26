import os
import time
import logging
import sys
import cx_Oracle
import asyncio
import httpx
import random
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))
from datetime import datetime
from queue import Queue
from threading import Thread
from logger_config import setup_logger

# ============================================================
# 初始化日志系统（必须在 config 之前！）
# ============================================================
setup_logger()

# ============================================================
# 导入依赖（在日志初始化之后）
# ============================================================
from config import *
from load_balancer_module import LoadBalancer

# ==== 配置 ====
logging.getLogger("httpx").setLevel(logging.WARNING)
os.environ["TNS_ADMIN"] = WALLET_DIR
sys.path.insert(0, str(Path(__file__).parent))

# ============================================
# 成员列表初始化逻辑
# ============================================
INSTANCE_ID = os.getenv("INSTANCE_ID")
MEMBER_ID = sys.argv[1] if len(sys.argv) > 1 else os.getenv("MEMBER_ID")
instance_count = 1
instance_index = 0

def slice_members_for_current_instance(all_members):
    """
    【中文备注 - 核心辅助函数】
    根据当前实例角色对成员列表进行切片分配。
    支持多检测器分片模式、单成员模式以及全量监控模式。
    确保系统在首次启动与后续运行时热加载成员配置时，分片规则完全一致，
    防止多检测器实例在热加载后被重置为全量成员检测导致流量翻倍。
    """
    if INSTANCE_ID:
        if instance_count <= 1:
            return all_members
        chunk_size = len(all_members) // instance_count
        start = instance_index * chunk_size
        end = start + chunk_size if instance_index < instance_count - 1 else len(all_members)
        return all_members[start:end]
    elif MEMBER_ID:
        if MEMBER_ID.upper() == "ALL":
            return all_members
        single = next((m for m in all_members if m["id"] == MEMBER_ID), None)
        return [single] if single else all_members
    else:
        return [all_members[0]] if all_members else []

# 模式1: 多检测器实例模式（自动检测）
if INSTANCE_ID:
    # 从实例ID提取索引 (monitor-a → 0, monitor-b → 1, ...)
    instance_index = ord(INSTANCE_ID[-1]) - ord('a')
    
    # ✅ 自动从数据库查询检测器实例总数
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM ADMIN.INSTANCES 
                WHERE INSTANCE_TYPE = 'monitor' 
                  AND STATUS = 'active'
            """)
            instance_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            if instance_count == 0:
                print(f"⚠️  警告: 数据库中没有活跃的检测器实例，使用单实例模式")
                instance_count = 1
        else:
            print(f"⚠️  警告: 无法连接数据库，使用单实例模式")
            instance_count = 1
    except Exception as e:
        print(f"⚠️  警告: 查询实例数失败 ({e})，使用单实例模式")
        instance_count = 1
    
    # 单实例模式（只有1台检测器）
    if instance_count == 1:
        MEMBERS = slice_members_for_current_instance(ENABLED_MEMBERS)
        print(f"✅ 单检测器模式: {INSTANCE_ID}")
        print(f"   监控所有成员: {len(MEMBERS)} 个")
    
    # 多实例模式（2台或更多）
    else:
        if instance_index >= instance_count:
            print(f"❌ 错误: 实例索引 {instance_index} 超出范围")
            print(f"   当前活跃实例数: {instance_count}")
            print(f"   请使用 monitor-a 到 monitor-{chr(ord('a') + instance_count - 1)}")
            sys.exit(1)
        
        # 计算分配范围（使用统一函数进行切片）
        MEMBERS = slice_members_for_current_instance(ENABLED_MEMBERS)
        chunk_size = len(ENABLED_MEMBERS) // instance_count
        start = instance_index * chunk_size
        end = start + chunk_size if instance_index < instance_count - 1 else len(ENABLED_MEMBERS)
        
        print(f"🔀 多检测器模式: {INSTANCE_ID}")
        print(f"   总实例数: {instance_count} (自动检测)")
        print(f"   本实例负责: {len(MEMBERS)} 个成员 (第 {start+1}-{end} 个)")
        print(f"   成员示例: {', '.join(m['id'] for m in MEMBERS[:3])}{'...' if len(MEMBERS) > 3 else ''}")

# 模式2: 传统模式（向后兼容）
elif MEMBER_ID:
    if MEMBER_ID.upper() == "ALL":
        MEMBERS = ENABLED_MEMBERS
        print(f"✅ 传统模式: 监控所有成员 ({len(MEMBERS)} 个)")
    else:
        MEMBER = next((m for m in ENABLED_MEMBERS if m["id"] == MEMBER_ID), None)
        if not MEMBER:
            print(f"❌ 错误: 找不到成员 ID: {MEMBER_ID}")
            print(f"   可用的成员: {', '.join(m['id'] for m in ENABLED_MEMBERS)}, ALL")
            sys.exit(1)
        MEMBERS = [MEMBER]
        print(f"✅ 单成员模式: 监控 {MEMBER['id']}")
else:
    MEMBERS = [ENABLED_MEMBERS[0]]
    print(f"⚠️  未指定成员，使用默认: {MEMBERS[0]['id']}")

# ==== 数据库队列 (不需要锁了,因为异步是单线程) ====
db_queue = Queue(maxsize=1000)

def reconnect_db(max_retries=3):
    """
    数据库重连函数，支持重试
    返回: (conn, cursor) 或 (None, None)
    """
    for attempt in range(max_retries):
        try:
            logging.info(f"[DB-Writer] 尝试重连数据库 (第 {attempt + 1}/{max_retries} 次)...")
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                # 测试连接是否有效
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.fetchone()
                logging.info(f"[DB-Writer] ✅ 数据库重连成功")
                return conn, cursor
        except Exception as e:
            logging.error(f"[DB-Writer] 重连失败 (第 {attempt + 1}/{max_retries} 次): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 指数退避: 1秒, 2秒, 4秒
    
    logging.error(f"[DB-Writer] ❌ 数据库重连失败，已尝试 {max_retries} 次")
    return None, None

# ==== 数据库连接 ====
def save_to_db(member_id, room_id, is_live_flag, started_at, prev_status, member):
    """将数据放入队列,由专门线程写入数据库"""
    db_queue.put({
        'member_id': member_id,
        'room_id': room_id,
        'is_live_flag': is_live_flag,
        'started_at': started_at,
        'prev_is_live': prev_status.get(member_id, {}).get('is_live', False),
        'group_name': member.get('group_name', ''),
        'team_name': member.get('team_name', '')
    })
    return True

def db_writer_thread(stop_flag):
    logging.info("[DB-Writer] 🚀 数据库写入线程启动 (实时全量模式)")
    conn = get_db_connection()
    cursor = None
    # ✅ 新增：初始化负载均衡器（用于给录制器分配）
    load_balancer = LoadBalancer(conn)
    # 新增：用于统计日志的变量
    total_processed_in_round = 0
    last_log_time = time.time()

    # ✅ 优化：将 SQL 语句定义在循环外，使用绑定变量，提高解析效率
    merge_sql = f"""
        MERGE /*+ NO_PARALLEL */ INTO {DB_TABLE} target
        USING (SELECT :member_id_param AS MEMBER_ID_VAL FROM DUAL) source
        ON (target.MEMBER_ID = source.MEMBER_ID_VAL)
        WHEN MATCHED THEN
            UPDATE SET ROOM_ID = :room_id_param, IS_LIVE = :live_flag_param,
                       STARTED_AT = NVL(:started_at_param, target.STARTED_AT), 
                       CHECK_TIME = :check_time_param, GROUP_NAME = :group_name_param, TEAM_NAME = :team_name_param
        WHEN NOT MATCHED THEN
            INSERT (MEMBER_ID, ROOM_ID, IS_LIVE, STARTED_AT, CHECK_TIME, GROUP_NAME, TEAM_NAME)
            VALUES (:member_id_param, :room_id_param, :live_flag_param, :started_at_param, :check_time_param, :group_name_param, :team_name_param)
    """
    
    insert_history_sql = f"INSERT INTO {DB_HISTORY_TABLE} (MEMBER_ID, ROOM_ID, STARTED_AT) VALUES (:member_id, :room_id, :started_at)"
    
    update_history_sql = f"""
        UPDATE {DB_HISTORY_TABLE}
        SET ENDED_AT = :ended_at,
            DURATION_MINUTES = ROUND(EXTRACT(DAY FROM (:ended_at - STARTED_AT)) * 1440 + EXTRACT(HOUR FROM (:ended_at - STARTED_AT)) * 60 + EXTRACT(MINUTE FROM (:ended_at - STARTED_AT)) + EXTRACT(SECOND FROM (:ended_at - STARTED_AT)) / 60, 2),
            UPDATED_AT = SYSTIMESTAMP
        WHERE ID = (SELECT MAX(ID) FROM {DB_HISTORY_TABLE} WHERE MEMBER_ID = :member_id AND ENDED_AT IS NULL)
    """
    # ✅ 心跳计数器（移到这里）
    last_check_time = time.time() # 改用时间戳
    
    while not stop_flag[0] or not db_queue.empty():
        # --- A. 检查并确保连接可用 ---
        current_time = time.time()  # ✅ 必须放在 if 之前定义
        
        # 只要连接没了，或者超过30秒没检查，就触发检测
        if conn is None or cursor is None or (current_time - last_check_time > 30):
            need_reconnect = False
            try:
                if conn and cursor:
                    # ✅ 主动心跳检测
                    cursor.execute("SELECT 1 FROM DUAL")
                    cursor.fetchone()
                    last_check_time = current_time # 只有成功才更新时间
                else:
                    need_reconnect = True
            except Exception as hb_error:
                logging.warning(f"[DB-Writer] 心跳检测失败: {hb_error}")
                need_reconnect = True

            # 执行重连逻辑
            if need_reconnect:
                logging.warning("[DB-Writer] 正在尝试恢复数据库连接...")
                conn, cursor = reconnect_db()
                if conn:
                    load_balancer = LoadBalancer(conn)
                    last_check_time = time.time() # 重连成功，重置时间
                else:
                    # ✅ 核心保护：连不上就休眠并跳过本次循环，数据会留在队列里
                    time.sleep(5)
                    continue
        try:
            # 1. 阻塞等待队列中的第一个数据，超时 1 秒
            data = db_queue.get(timeout=1.0)
            batch_buffer = [data]
            # 2. 【核心】瞬间排空队列里剩余的所有数据 (这 277 条会瞬间被拿出来)
            while not db_queue.empty():
                try:
                    batch_buffer.append(db_queue.get_nowait())
                except:
                    break
                
            # 3. 按 member_id 去重，只保留本轮最新的状态
            unique_buffer = {d['member_id']: d for d in batch_buffer}
            final_list = list(unique_buffer.values())
            # 4. 执行批量操作
            if final_list and conn:
                if cursor is None: cursor = conn.cursor()
                all_bind_params = []
                history_inserts = []
                history_updates = []
                check_time = datetime.now()
                for d in final_list:
                    all_bind_params.append({
                        'member_id_param': d['member_id'],
                        'room_id_param': d['room_id'],
                        'live_flag_param': 1 if d['is_live_flag'] else 0,
                        'started_at_param': d['started_at'],
                        'check_time_param': check_time,
                        'group_name_param': d['group_name'],
                        'team_name_param': d['team_name']
                    })
                    # 历史表逻辑
                    if d['is_live_flag'] and not d['prev_is_live']:
                        # 开播：插入历史记录
                        history_inserts.append({
                            'member_id': d['member_id'], 
                            'room_id': d['room_id'], 
                            'started_at': d['started_at']
                        })
                        # ✅ 新增：立即分配录制器
                        try:
                            recorder_id = load_balancer.assign_recorder(d['member_id'])
                            if recorder_id:
                                logging.info(f"[分配] {d['member_id']} → {recorder_id}")
                        except Exception as e:
                            logging.error(f"[分配失败] {d['member_id']}: {e}")
                    elif not d['is_live_flag'] and d['prev_is_live']:
                        # 下播：更新历史记录
                        history_updates.append({
                            'ended_at': check_time, 
                            'member_id': d['member_id']
                        })
                        # ✅ 新增：清除分配
                        try:
                            load_balancer.clear_assignment(d['member_id'])
                            logging.debug(f"[清除分配] {d['member_id']}")
                        except Exception as e:
                            logging.error(f"[清除失败] {d['member_id']}: {e}")
                # 5. 一次性写入并提交 (这是 277 条数据最快的入库方式)
                cursor.executemany(merge_sql, all_bind_params)
                if history_inserts: cursor.executemany(insert_history_sql, history_inserts)
                if history_updates: cursor.executemany(update_history_sql, history_updates)
                conn.commit()
                # ✅ 累加处理数量，但不立刻打日志
                total_processed_in_round += len(final_list)
            # 4. 重点：判断是否达到 5 秒的日志周期
            current_time = time.time()
            if current_time - last_log_time >= 5.0:
                if total_processed_in_round > 0:
                    logging.info(f"✅ [周期汇总] 过去 5 秒内实时入库共计: {total_processed_in_round} 条记录")
                # 重置计数器
                total_processed_in_round = 0
                last_log_time = current_time
            # 标记完成
            for _ in range(len(batch_buffer)):
                db_queue.task_done()
        except Exception as e:
            if 'data' in locals():
                error_obj = None
                # 判断是否是数据库连接错误
                if isinstance(e, cx_Oracle.DatabaseError):
                    error_obj, = e.args
                    logging.error(f"[DB-Writer] 数据库错误: {error_obj.code} - {error_obj.message}")
                else:
                    logging.error(f"[DB-Writer] 数据库写入错误: {e}")
                # 安全回滚
                try:
                    if conn:
                        conn.rollback()
                except Exception as rollback_error:
                    logging.warning(f"[DB-Writer] 回滚失败（连接可能已断开）: {rollback_error}")
                # 🔥 关键：如果是连接错误，尝试重连
                if error_obj and error_obj.code in [3113, 3114, 1089, 1090, 28, 12535, 12537]:
                    logging.warning(f"[DB-Writer] 检测到连接错误 (ORA-{error_obj.code})，开始重连...")
                    # 关闭旧连接
                    try:
                        if cursor:
                            cursor.close()
                        if conn:
                            conn.close()
                    except:
                        pass
                    
                    # 重连
                    conn, cursor = reconnect_db()
                    # 重新初始化 LoadBalancer
                    if conn:
                        try:
                            load_balancer = LoadBalancer(conn)
                            logging.info("[DB-Writer] LoadBalancer 已重新初始化")
                        except Exception as lb_error:
                            logging.error(f"[DB-Writer] LoadBalancer 初始化失败: {lb_error}")
                    else:
                        logging.error("[DB-Writer] ❌ 重连失败，将在下次循环继续尝试")
                        time.sleep(5)  # 等待5秒再继续
            continue

    # ✅ while 循环结束后，线程退出前处理剩余数据
    if batch_buffer and conn and cursor:
        try:
            logging.info(f"[DB-Writer] 🔄 处理退出前剩余的 {len(batch_buffer)} 条数据")
            # ✅ 在循环外准备批量参数列表
            all_bind_params = []
            history_inserts = []
            history_updates = []

            for data in batch_buffer:
                member_id = data['member_id']
                room_id = data['room_id']
                is_live_flag = data['is_live_flag']
                started_at = data['started_at']
                prev_is_live = data['prev_is_live']
                check_time = datetime.now()
                
                # 收集主表参数
                all_bind_params.append({
                    'member_id_param': member_id,
                    'room_id_param': room_id,
                    'live_flag_param': 1 if is_live_flag else 0,
                    'started_at_param': started_at,
                    'check_time_param': check_time,
                    'group_name_param': data['group_name'],
                    'team_name_param': data['team_name']
                })
                
                # 收集历史表操作
                if is_live_flag and not prev_is_live:
                    history_inserts.append({
                        'member_id': member_id,
                        'room_id': room_id,
                        'started_at': started_at
                    })
                    
                    # ✅ 新增：分配录制器
                    try:
                        recorder_id = load_balancer.assign_recorder(member_id)
                        if recorder_id:
                            logging.info(f"[退出前分配] {member_id} → {recorder_id}")
                    except Exception as e:
                        logging.error(f"[退出前分配失败] {member_id}: {e}")
                        
                elif not is_live_flag and prev_is_live:
                    history_updates.append({
                        'ended_at': check_time,
                        'member_id': member_id
                    })
                    
                    # ✅ 新增：清除分配
                    try:
                        load_balancer.clear_assignment(member_id)
                    except Exception as e:
                        logging.error(f"[退出前清除失败] {member_id}: {e}")

            # ✅ 批量执行 - 只调用一次!
            if all_bind_params:
                cursor.executemany(merge_sql, all_bind_params)
            
            if history_inserts:
                cursor.executemany(insert_history_sql, history_inserts)
                logging.info(f"批量插入 {len(history_inserts)} 条开播记录")
            
            if history_updates:
                cursor.executemany(update_history_sql, history_updates)
                logging.info(f"批量更新 {len(history_updates)} 条结束记录")
            
            conn.commit()
            logging.info(f"[DB-Writer] ✅ 退出前提交剩余 {len(batch_buffer)} 条")
        except Exception as e:
            logging.error(f"退出前提交失败: {e}")
    
    # ✅ 线程退出时清理资源
    if cursor:
        try:
            cursor.close()
        except:
            pass
    if conn:
        try:
            conn.close()
        except:
            pass
    
    logging.info("[DB-Writer] 数据库写入线程已停止")

# ==== 异步HTTP请求 ====
async def is_live_async(member_id, room_url_key, client):
    """异步检查直播状态"""
    url = f"https://www.showroom-live.com/api/room/status?room_url_key={room_url_key}"

    try:
        res = await client.get(url)
        if res.status_code != 200:
            logging.warning(f"[{member_id}] 请求异常: {res.status_code}")
            return None, None
        
        try:
            data = res.json()
        except ValueError:
            logging.warning(f"[{member_id}] 返回非JSON内容,可能被限流")
            return None, None

        is_live_flag = data.get("is_live", False)
        started_at_raw = data.get("started_at") if is_live_flag else None
        
        if started_at_raw:
            started_at = datetime.fromtimestamp(started_at_raw)
        else:
            started_at = None
        
        return is_live_flag, started_at
    except Exception as e:
        logging.exception(f"[{member_id}] 获取直播状态失败")
        return False, None

def generate_key(member):
    """生成room_url_key"""
    name_en = member.get("name_en", "")
    parts = name_en.split(" ")
    if len(parts) == 2:
        key_suffix = f"{parts[1]}_{parts[0]}" 
    else:
        key_suffix = name_en.replace(" ", "_")
    return f"48_{key_suffix}"

async def check_single_member(member, client, previous_status, last_db_write_time):
    member_id = member["id"]
    room_id = member["room_id"]
    name_jp = member["name_jp"]
    room_url_key = member.get("room_url_key") or generate_key(member)
    
    # 1. 异步获取当前直播状态
    is_live_flag, started_at = await is_live_async(member_id, room_url_key, client)
    
    if is_live_flag is not None:
        # 获取上一次的状态
        prev_record = previous_status.get(member_id, {})
        
        # 直接写入数据库，不再判断 60 秒心跳
        save_to_db(member_id, room_id, is_live_flag, started_at, previous_status, member)
        
        # 更新内存状态
        previous_status[member_id] = {'is_live': is_live_flag, 'started_at': started_at}

async def check_all_members_async(members, ip_clients, previous_status, last_db_write_time):
    """
    动态平滑并发：确保请求在均匀分布，且每个 IP 瞬时只负责一个成员
    """
    total_members = len(members)
    if total_members == 0:
        return

    # 1. 计算步进间隔
    target_fill_time = (REQUEST_INTERVAL - 0.1)
    interval = target_fill_time / total_members

    # 2. 限制总并发数为 IP 数量，确保资源不超载
    sem = asyncio.Semaphore(len(ip_clients))

    # 3. 【关键】每一轮都生成一个随机顺序的 IP 客户端列表
    # 这样可以打破“成员A 永远用 IP_1”的固定关系
    shuffled_clients = ip_clients.copy()
    random.shuffle(shuffled_clients)

    async def throttled_check(member, client, index):
        # 按计算好的时间点出发，实现平滑请求
        await asyncio.sleep(index * interval)
        
        async with sem:
            # 在信号量保护下，由于 client 是按 index % len 分配的，
            # 配合信号量大小等于 IP 总数，可以保证此时该 IP 没有被其他任务占用
            return await check_single_member(member, client, previous_status, last_db_write_time)

    tasks = []
    num_ips = len(shuffled_clients)
    
    for i, member in enumerate(members):
        # 4. 【核心】使用打乱后的 IP 列表进行轮询
        # 假设有 30 个 IP，那么 i=0..29 时，每个成员分配到的 IP 绝对不重复
        client = shuffled_clients[i % num_ips]
        tasks.append(throttled_check(member, client, i))

    # 并发执行
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 错误统计
    errors = [r for r in results if isinstance(r, Exception)]
    if errors:
        logging.warning(f"本轮完成，其中 {len(errors)} 个请求发生代码级异常")

async def monitor_loop_async():
    """异步主循环"""
    global MEMBERS
    logging.info(f"🚀 开始监视 {len(MEMBERS)} 个主播 (异步模式)")
    
    previous_status = {}
    last_db_write_time = {}
    stop_flag = [False]

    # ✅ 预处理所有成员的team信息
    for member in MEMBERS:
        team_full = member.get("team", "")
        team_parts = team_full.split(" ", 1)
        member['group_name'] = team_parts[0] if len(team_parts) > 0 else ""
        member['team_name'] = team_parts[1] if len(team_parts) > 1 else ""
    
    # ✅ 直接创建客户端列表
    ip_clients = []

    for ip in OUTBOUND_IPS:
        client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(local_address=ip),
            timeout=10.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=15)
        )
        client._bound_ip = ip
        ip_clients.append(client)
    
    logging.info(f"✅ 创建 {len(ip_clients)} 个异步HTTP客户端")
    logging.info(f"📊 配置信息:")
    logging.info(f"   总成员: {len(MEMBERS)}")
    logging.info(f"   IP池大小: {len(OUTBOUND_IPS)}")
    logging.info(f"   并发请求数: {len(MEMBERS)}")
    
    # 启动DB线程
    db_thread = Thread(
        target=db_writer_thread,
        args=(stop_flag,),
        name="DB-Writer",
        daemon=True
    )
    db_thread.start()
    
    # 主循环监控
    try:
        loop_count = 0
        while not stop_flag[0]:
            round_start = time.time()
            
            # 重新加载成员配置（支持运行时动态更新成员，同时保持实例分片不变）
            try:
                from config import get_enabled_members
                all_members = get_enabled_members()
                if all_members:
                    # 【中文备注 - 核心修复】
                    # 重新切片成员列表，避免多实例模式在热加载后被全量覆盖导致各实例重复监控
                    MEMBERS = slice_members_for_current_instance(all_members)
                    # 重新加载后预处理队伍与分组信息
                    for member in MEMBERS:
                        team_full = member.get("team", "")
                        team_parts = team_full.split(" ", 1)
                        member['group_name'] = team_parts[0] if len(team_parts) > 0 else ""
                        member['team_name'] = team_parts[1] if len(team_parts) > 1 else ""
            except Exception as e:
                logging.error(f"重新加载成员配置失败: {e}")
            
            # ✅ 定期清理过期状态 (每100轮)
            if loop_count % 100 == 0 and loop_count > 0:
                current_ids = {m['id'] for m in MEMBERS}
                old_count = len(previous_status)
                previous_status = {k: v for k, v in previous_status.items() if k in current_ids}
                last_db_write_time = {k: v for k, v in last_db_write_time.items() if k in current_ids}
                if old_count > len(previous_status):
                    logging.info(f"🧹 清理了 {old_count - len(previous_status)} 个过期状态")
            
            # 并发检测所有成员
            await check_all_members_async(MEMBERS, ip_clients, previous_status, last_db_write_time)
            
            round_time = time.time() - round_start
            loop_count += 1

            if round_time < REQUEST_INTERVAL:
                await asyncio.sleep(REQUEST_INTERVAL - round_time)

            queue_size = db_queue.qsize()
            logging.info(f"⏱️ 轮询完成:耗时 {round_time:.2f} 秒 | 队列: {queue_size}")
            
            if queue_size > 800:
                logging.warning(f"⚠️ 队列堆积: {queue_size}/1000")
                
    except KeyboardInterrupt:
        logging.info("收到停止信号...")
    except Exception as e:
        logging.error(f"主循环异常: {e}", exc_info=True)
    finally:
        stop_flag[0] = True
        
        logging.info("等待数据库队列清空...")
        try:
            db_queue.join()
            logging.info("✅ 队列已清空")
        except Exception as e:
            logging.warning(f"⚠️ 队列清空失败: {e}")
        
        db_thread.join(timeout=10)
        if db_thread.is_alive():
            logging.warning("⚠️ DB线程未能正常退出")
        
        # ✅ 关闭所有异步客户端
        logging.info("关闭HTTP客户端...")
        for client in ip_clients:
            try:
                await client.aclose()
            except Exception as e:
                logging.error(f"关闭客户端失败: {e}")
        
        logging.info("所有任务已停止")

if __name__ == "__main__":
    try:
        asyncio.run(monitor_loop_async())
    except KeyboardInterrupt:
        # 这里什么都不写，或者只打印一行简单的退出提示
        pass