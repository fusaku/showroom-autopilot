import os
import time
import requests
import logging
import sys
import cx_Oracle
from pathlib import Path
from datetime import datetime
from config import WALLET_DIR
from requests_toolbelt import SourceAddressAdapter
from threading import Thread, Lock
from queue import Queue
from logger_config import setup_logger

# ==== 配置 ====
os.environ["TNS_ADMIN"] = WALLET_DIR

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    ENABLED_MEMBERS, LOG_DIR, REQUEST_INTERVAL, 
    DB_USER, DB_PASSWORD, DB_TABLE, TNS_ALIAS, DB_HISTORY_TABLE,
    OUTBOUND_IPS
)

MEMBER_ID = sys.argv[1] if len(sys.argv) > 1 else os.getenv("MEMBER_ID")

if MEMBER_ID:
    if MEMBER_ID.upper() == "ALL":
        MEMBERS = ENABLED_MEMBERS
        print(f"监控所有成员: {', '.join(m['id'] for m in MEMBERS)}")
    else:
        MEMBER = next((m for m in ENABLED_MEMBERS if m["id"] == MEMBER_ID), None)
        if not MEMBER:
            print(f"错误: 找不到成员 ID: {MEMBER_ID}")
            print(f"可用的成员: {', '.join(m['id'] for m in ENABLED_MEMBERS)}, ALL")
            sys.exit(1)
        MEMBERS = [MEMBER]
        print(f"监控单个成员: {MEMBER['id']}")
else:
    MEMBERS = [ENABLED_MEMBERS[0]]
    print(f"未指定 MEMBER_ID,使用默认成员: {MEMBERS[0]['id']}")

# ==== 线程安全的锁 ====
status_lock = Lock()
db_queue = Queue(maxsize=1000)

# ==== 数据库连接 ====
def get_db_connection():
    """获取Oracle数据库连接"""
    try:
        conn = cx_Oracle.connect(user=DB_USER, password=DB_PASSWORD, dsn=TNS_ALIAS)
        return conn
    except Exception as e:
        logging.error(f"Oracle数据库连接失败: {e}")
        return None

def save_to_db(member_id, room_id, is_live_flag, started_at, prev_status, member):
    """将数据放入队列,由专门线程写入数据库"""
    # ✅ 只提取之前的 is_live 状态 (布尔值)
    prev_is_live = prev_status.get(member_id, {}).get('is_live', False)
    team_full = member.get("team", "") if member else ""
    # 拆分team（假设格式是 "GROUP TEAM"）
    team_parts = team_full.split(" ", 1)
    group_name = team_parts[0] if len(team_parts) > 0 else ""
    team_name = team_parts[1] if len(team_parts) > 1 else ""

    db_queue.put({
        'member_id': member_id,
        'room_id': room_id,
        'is_live_flag': is_live_flag,
        'started_at': started_at,
        'prev_is_live': prev_is_live,  # ✅ 只传布尔值
        'group_name': group_name,  # ✅ 新增
        'team_name': team_name      # ✅ 新增
    })
    return True

def db_writer_thread(stop_flag):
    logging.info("[DB-Writer] 🚀 数据库写入线程启动")
    conn = None # 🆕 连接初始化为 None
    
    while not stop_flag[0]:
        # 🆕 检查并尝试获取/恢复连接
        if conn is None:
            conn = get_db_connection()
            if conn is None:
                time.sleep(5) # 休息5秒再尝试连接
                continue
            try:
                cursor = conn.cursor()
            except Exception as e:
                logging.error(f"无法创建 cursor: {e}")
                conn.close()
                conn = None
                continue
        data = None 
        
        try:
            data = db_queue.get(timeout=1)
            
            # ✅ 提取数据
            member_id = data['member_id']
            room_id = data['room_id']
            is_live_flag = data['is_live_flag']
            started_at = data['started_at']
            prev_is_live = data['prev_is_live']  # ✅ 改成直接用布尔值
            group_name = data['group_name']
            team_name = data['team_name']

            # 2. 执行数据库写入逻辑 (使用内部 try/finally 处理连接和操作)
            try:                
                # 数据库操作主体
                live_flag_value = 1 if is_live_flag else 0
                check_time = datetime.now()
                
                # 1. 更新当前状态表 (Merge SQL 保持不变)
                merge_sql = f"""
                    MERGE INTO {DB_TABLE} target
                    USING (SELECT :member_id_param AS MEMBER_ID_VAL,
                                  :room_id_param AS ROOM_ID_VAL,
                                  :live_flag_param AS IS_LIVE_VAL,
                                  :started_at_param AS STARTED_AT_VAL,
                                  :check_time_param AS CHECK_TIME_VAL,
                                  :group_name_param AS GROUP_NAME_VAL,
                                  :team_name_param AS TEAM_NAME_VAL
                           FROM DUAL) source
                    ON (target.MEMBER_ID = source.MEMBER_ID_VAL)
                    
                    WHEN MATCHED THEN
                        UPDATE SET
                            target.ROOM_ID     = source.ROOM_ID_VAL,
                            target.IS_LIVE     = source.IS_LIVE_VAL,
                            target.STARTED_AT  = NVL(source.STARTED_AT_VAL, target.STARTED_AT), 
                            target.CHECK_TIME  = source.CHECK_TIME_VAL,
                            target.GROUP_NAME  = source.GROUP_NAME_VAL,
                            target.TEAM_NAME   = source.TEAM_NAME_VAL

                    WHEN NOT MATCHED THEN
                        INSERT (MEMBER_ID, ROOM_ID, IS_LIVE, STARTED_AT, CHECK_TIME, GROUP_NAME, TEAM_NAME)
                        VALUES (source.MEMBER_ID_VAL, source.ROOM_ID_VAL, source.IS_LIVE_VAL, 
                                source.STARTED_AT_VAL, source.CHECK_TIME_VAL,
                                source.GROUP_NAME_VAL, source.TEAM_NAME_VAL)
                """
                
                params = {
                    'member_id_param': member_id,
                    'room_id_param': room_id,
                    'live_flag_param': live_flag_value,
                    'started_at_param': started_at,
                    'check_time_param': check_time,
                    'group_name_param': group_name,  # ✅ 新增
                    'team_name_param': team_name      # ✅ 新增
                }
                
                try:
                    cursor.execute(merge_sql, params)
                    
                    # 2. 维护历史记录表
                    if is_live_flag and not prev_is_live:
                        insert_history_sql = f"""
                            INSERT INTO {DB_HISTORY_TABLE} (MEMBER_ID, ROOM_ID, STARTED_AT)
                            VALUES (:member_id, :room_id, :started_at)
                        """
                        cursor.execute(insert_history_sql, {
                            'member_id': member_id,
                            'room_id': room_id,
                            'started_at': started_at
                        })
                        logging.info(f"[{member_id}] ✅ 直播开始,已记录到历史表 (started_at: {started_at})")
                    
                    elif not is_live_flag and prev_is_live:
                        update_history_sql = f"""
                            UPDATE {DB_HISTORY_TABLE}
                            SET ENDED_AT = :ended_at,
                                DURATION_MINUTES = ROUND(
                                    EXTRACT(DAY FROM (:ended_at - STARTED_AT)) * 24 * 60 +
                                    EXTRACT(HOUR FROM (:ended_at - STARTED_AT)) * 60 +
                                    EXTRACT(MINUTE FROM (:ended_at - STARTED_AT)) +
                                    EXTRACT(SECOND FROM (:ended_at - STARTED_AT)) / 60, 2
                                ),
                                UPDATED_AT = SYSTIMESTAMP
                            WHERE ID = (
                                SELECT MAX(ID)
                                FROM {DB_HISTORY_TABLE}
                                WHERE MEMBER_ID = :member_id
                                  AND ENDED_AT IS NULL
                            )
                        """
                        cursor.execute(update_history_sql, {
                            'ended_at': check_time, 
                            'member_id': member_id
                        })
                        logging.info(f"[{member_id}] ✅ 直播结束,已更新历史表 (ended_at: {check_time})")
                    
                    conn.commit()
                except Exception as e:
                    # ... (错误处理逻辑，确保关闭 conn 和 cursor)
                    if cursor:
                        cursor.close()
                    cursor = None # ⚠️ 设置为 None
                    conn = None   # ⚠️ 设置为 None
            
            except Exception as e:
                # 捕获数据库操作错误
                logging.error(f"数据库操作错误,尝试回滚并断开连接: {e}")
                if conn:
                    try:
                        conn.rollback()
                        conn.close() # 失败后主动关闭连接，触发下次循环的重连
                    except:
                        pass
                conn = None # ⚠️ 设置为 None，下次循环会尝试重新连接
                logging.error(f"数据库写入错误: {e}")
        except:
                continue
        finally:
            if data is not None:
                try:
                    db_queue.task_done()
                except:
                    pass
        # 线程结束时，确保连接关闭
    if conn:
        try:
            conn.close()
        except:
            pass
    
    logging.info("[DB-Writer] 数据库写入线程已停止")

def is_live(member_id, room_url_key, session):  # ✅ 改成接收 session 参数
    """检查直播状态"""
    # 不再添加硬编码的 "48_" 前缀
    url = f"https://www.showroom-live.com/api/room/status?room_url_key={room_url_key}"

    try:
        # ✅ 直接用传入的 session,不要再创建
        res = session.get(url, timeout=10)
        if res.status_code != 200:
            logging.warning(f"[{member_id}] 请求异常: {res.status_code}")
            return None, None  # 用 None 表示“无法获取状态”，而不是 False
        try:
            data = res.json()
        except ValueError:
            logging.warning(f"[{member_id}] 返回非 JSON内容，可能被限流")
            return None, None

        is_live_flag = data.get("is_live", False)
        started_at_raw = data.get("started_at") if is_live_flag else None
        
        if started_at_raw:
            started_at = datetime.fromtimestamp(started_at_raw)
        else:
            started_at = None
        
        return is_live_flag, started_at  # ✅ 不需要返回 source_ip 了
    except Exception as e:
        logging.exception(f"[{member_id}] 获取直播状态失败")
        return False, None

def worker_thread(ip, ip_index, members_subset, previous_status, stop_flag, target_cycle_time):

    if not members_subset:
        logging.info(f"未分配成员,线程将不执行检测")
        return
    
    session = requests.Session()
    # ✅ 创建自定义 adapter,限制连接池
    adapter = SourceAddressAdapter(ip)
    adapter.pool_connections = 1   # ✅ 只缓存1个host的连接池 (你只访问showroom-live.com)
    adapter.pool_maxsize = 2       # ✅ 每个池最多2个连接
    
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    try:
        # ⏱️ 计算延迟启动时间
        stagger_delay = (target_cycle_time / len(OUTBOUND_IPS)) * ip_index
        logging.info(f"将在 {stagger_delay:.1f} 秒后启动, 负责 {len(members_subset)} 个主播")
        time.sleep(stagger_delay)

        logging.info(f"🚀 开始工作")

        while not stop_flag[0]:  # 检查停止标志
            round_start = time.time()

            # ✅ 每轮开始时重新加载成员配置
            try:
                from config import get_enabled_members
                all_members = get_enabled_members()
                
                # 重新计算当前线程负责的成员
                num_ips = len(OUTBOUND_IPS)
                my_members = [m for i, m in enumerate(all_members) if i % num_ips == ip_index]
                members_subset = my_members  # 更新本地变量
                
            except Exception as e:
                logging.error(f"重新加载成员配置失败: {e},继续使用旧配置")
        
            # 检查所有成员
            for i, member in enumerate(members_subset):
                name_en = member["name_en"]
                member_id = member["id"]
                room_id = member["room_id"]
                name_jp = member["name_jp"]
                room_url_key = member.get("room_url_key") # ✅ 尝试从配置中获取新的 key
                
                if not room_url_key:
                    # ⚠️ 后备逻辑：如果配置中没有 room_url_key，使用原先的推导逻辑作为后备
                    # 此时必须补上 48_ 前缀
                    parts = name_en.split(" ")
                    if len(parts) == 2:
                        # 原始逻辑: parts[1]_parts[0] -> (Haruna_Hashimoto)
                        key_suffix = f"{parts[1]}_{parts[0]}" 
                    else:
                        key_suffix = name_en.replace(" ", "_")
                    room_url_key = f"48_{key_suffix}" # 补上 48_
                    logging.warning(f"[{member_id}] 配置缺少 room_url_key, 使用推导值: {room_url_key}")
                
                # 传入 member_id, room_url_key, session
                # ⚠️ 修改调用，传入 room_url_key 替代 name_en
                is_live_flag, started_at = is_live(member_id, room_url_key, session)

                # 保存到数据库
                save_to_db(member_id, room_id, is_live_flag, started_at, previous_status, member)

                #更新状态记录
                with status_lock:
                    previous_status[member_id] = {
                        'is_live': is_live_flag,
                        'started_at': started_at
                    }

                if is_live_flag:
                    logging.info(f"[{name_jp}] 正在直播中 (开始时间: {started_at})")
                else:
                    logging.debug(f"[{name_jp}] 当前未直播")

                # 每检测一个成员后等待
                if i < len(members_subset) - 1:
                    time.sleep(REQUEST_INTERVAL)

            round_time = time.time() - round_start
            logging.info(f"✅ 本轮检测完成,耗时 {round_time:.2f} 秒")

            # 🆕 使用动态计算的目标周期时间
            TARGET_CYCLE_TIME = target_cycle_time
            wait_time = max(0, TARGET_CYCLE_TIME - round_time)
            
            if wait_time > 0:
                logging.info(f"🚀 统一周期 ⏳ 等待 {wait_time:.2f} 秒后开始下一轮 (周期:{TARGET_CYCLE_TIME:.1f}s)...")
            else:
                 # ⚠️ round_time > 4.6 秒时，发出警告
                 logging.warning(f"⚠️ 线程落后 {abs(wait_time):.2f} 秒，立即开始下一轮! (请检查 REQUEST_INTERVAL)")

            if wait_time > 0:
                time.sleep(wait_time)
    finally:  # ✅ 确保关闭
        session.close()
        logging.info(f"Session 已关闭")

def monitor_loop():
    logging.info(f"🚀 开始监视 {len(MEMBERS)} 个主播 (使用 {len(OUTBOUND_IPS)} 个IP错开轮询)")
    logging.info(f"IP列表: {', '.join(OUTBOUND_IPS)}")
    logging.info(f"⏱️  每个IP间隔 {30 / len(OUTBOUND_IPS):.1f} 秒启动")
    
    previous_status = {}
    stop_flag = [False]
    # --- 🆕 优先级划分和任务分配 ---
    num_ips = len(OUTBOUND_IPS)

        # 1. 初始化分配列表
    member_subsets = [[] for _ in range(num_ips)] 
    
    # 2. 将所有 MEMBERS (包括 M0) 均匀分配给所有 10 个 IP
    for i, member in enumerate(MEMBERS):
        # i % num_ips 得到 0 到 9 的索引，均匀分配所有 46 个成员
        target_ip_index = i % num_ips 
        member_subsets[target_ip_index].append(member)

    # 🆕 动态计算最佳周期时间: (总成员数 / IP数) * 安全系数
    TARGET_CYCLE_TIME = (len(MEMBERS) / num_ips) * 1.05  # 增加 5% 作为安全冗余
    logging.info(f"所有 {len(MEMBERS)} 个成员已均匀分配给 {num_ips} 个IP。")
    logging.info(f"系统已切换到 {TARGET_CYCLE_TIME:.2f} 秒周期，所有成员的最长发现延迟均约为 {TARGET_CYCLE_TIME:.2f} 秒。")

    # 🆕 启动数据库写入线程
    db_thread = Thread(
        target=db_writer_thread,
        args=(stop_flag,),
        name="DB-Writer",
        daemon=True
    )
    db_thread.start()
    
    # 为每个IP创建一个线程,并错开启动
    threads = []
    for ip_index, ip in enumerate(OUTBOUND_IPS):
        t = Thread(
            target=worker_thread, 
            args=(ip, ip_index, member_subsets[ip_index], previous_status, stop_flag, TARGET_CYCLE_TIME), # 🆕 传递动态周期时间
            name=f"IP-{ip}",
            daemon=True
        )
        t.start()
        threads.append(t)
    
    try:
        loop_count = 0  # ✅ 放在外面
        while True:     # ✅ 只要一层 while
            time.sleep(10)
            loop_count += 1
            
            queue_size = db_queue.qsize()
            
            # 每1分钟(6次循环)输出一次状态
            if loop_count % 6 == 0:
                logging.info(f"📊 队列状态: {queue_size} 个待处理任务")
            
            # ⚠️ 队列堆积预警
            if queue_size > 800:
                logging.warning(f"⚠️ 队列堆积严重: {queue_size}/1000,数据库可能处理过慢!")
                
    except KeyboardInterrupt:
        logging.info("收到停止信号,正在关闭...")
        stop_flag[0] = True
        
        # 等待队列清空
        logging.info("等待数据库队列清空...")
        try:
            db_queue.join(timeout=30)  # ✅ 最多等30秒
            logging.info("✅ 队列已清空")
        except:
            logging.warning(f"⚠️ 队列未完全清空,剩余 {db_queue.qsize()} 个任务")
        
        db_thread.join(timeout=5)
        for t in threads:
            t.join(timeout=5)
        logging.info("所有线程已停止")

if __name__ == "__main__":
    setup_logger(LOG_DIR, "monitor_showroom")
    monitor_loop()