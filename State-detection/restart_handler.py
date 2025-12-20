import os
import time
import logging
import sys
import cx_Oracle
from pathlib import Path
from datetime import datetime, timedelta 
from logger_config import setup_logger
from config import (
    ENABLED_MEMBERS, 
    RESTART_CHECK_INTERVAL, 
    MIN_RESTART_INTERVAL,
    TS_PARENT_DIR,
    LOG_DIR, 
    GRACEFUL_START_DELAY,
    WALLET_DIR,
    DB_USER,
    DB_PASSWORD,
    DB_TABLE,
    TNS_ALIAS
)
# **全局变量**
# ✅ 修改配置常量：TS文件多久没有更新视为录制停止，改为 60 秒
MAX_TS_INACTIVE_TIME = 60 # 60 秒
GLOBAL_CONN = None 
os.environ["TNS_ADMIN"] = WALLET_DIR

setup_logger(LOG_DIR, "restart_handler")

"""获取Oracle数据库连接"""
def connect_db():
    """尝试建立或重新建立 Oracle 数据库连接。"""
    global GLOBAL_CONN
    
    # 如果已存在连接，先尝试关闭它
    if GLOBAL_CONN:
        try:
            GLOBAL_CONN.close()
            logging.info("旧的数据库连接已关闭。尝试重连...")
        except Exception:
            pass 

    try:
        GLOBAL_CONN = cx_Oracle.connect(user=DB_USER, password=DB_PASSWORD, dsn=TNS_ALIAS)
        logging.info("Oracle数据库持久连接成功建立/重新连接成功。")
        return True
    except Exception as e:
        # 这里只记录错误，但不退出
        logging.error(f"Oracle数据库连接失败: {e}")
        GLOBAL_CONN = None 
        return False

# 首次尝试连接
if not connect_db():
    logging.critical("首次数据库连接失败，脚本退出。")
    sys.exit(1)

MEMBER_ID = os.getenv("MEMBER_ID")

if MEMBER_ID:
    MEMBER = next((m for m in ENABLED_MEMBERS if m["id"] == MEMBER_ID), None)
    if not MEMBER:
        print(f"错误: 找不到成员 ID: {MEMBER_ID}")
        sys.exit(1)
else:
    MEMBER = ENABLED_MEMBERS[0]
    print(f"未指定 MEMBER_ID，使用默认成员: {MEMBER['id']}")

SERVICE_NAME = f"showroom-{MEMBER['id']}.service"

last_restart_time = 0

def read_live_status():
    """
    从数据库读取直播状态。
    使用全局持久连接 GLOBAL_CONN，因此查询后不关闭连接。
    """
    # 直接使用全局连接
    conn = GLOBAL_CONN
    
    try:
        # 使用 with 语句确保游标会被自动关闭
        with conn.cursor() as cursor:
            
            # 查询当前成员的状态
            query = f"""
                SELECT IS_LIVE, STARTED_AT
                FROM {DB_TABLE}
                WHERE MEMBER_ID = :member_id
            """
            
            # 使用绑定变量防止 SQL 注入
            cursor.execute(query, {'member_id': MEMBER['id']})
            result = cursor.fetchone()
            
            if result:
                is_live = bool(result[0])  # IS_LIVE 字段 (1=True, 0=False)
                started_at = None
                
                if is_live and result[1]:  # STARTED_AT 字段
                    # 假定 cx_Oracle 返回的是 datetime 对象
                    if isinstance(result[1], datetime):
                        started_at = int(result[1].timestamp())
                    else:
                        # 以防万一，尝试将其他类型（如数字字符串）转换为 int
                        try:
                            started_at = int(result[1])
                        except (TypeError, ValueError):
                            logging.error(f"STARTED_AT 字段类型或值错误: {result[1]}")
                            started_at = None

                logging.debug(f"从数据库读取状态: is_live={is_live}, started_at={started_at}")
                return is_live, started_at
            else:
                logging.warning(f"数据库中未找到成员 {MEMBER['id']} 的记录")
                return False, None
    except cx_Oracle.Error as e:
            # 捕获 Oracle 数据库错误，这通常意味着连接断开或会话失效
            logging.error(f"从数据库读取状态失败（连接可能失效）: {e}")

            # 尝试重连
            logging.warning("尝试重新建立数据库连接...")
            if connect_db():
                # 重连成功，虽然本次读取失败，但下次循环应能恢复
                logging.info("数据库连接已恢复。")
            else:
                # 重连失败，需要等待下次循环或重启
                logging.error("数据库重连失败。")

            return False, None
    except Exception as e:
        # 捕获其他非 cx_Oracle 错误 (如程序逻辑错误)
        logging.error(f"读取状态时发生非数据库异常: {e}")
        return False, None

def get_latest_subfolder(parent: Path):
    """
    ✅ 跨日修改: 检查今天和昨天的日期字符串，以支持跨日直播
    """
    
    # 获取当前监控成员的英文名用于文件夹匹配
    member_name_in_folder = MEMBER.get('name_en', MEMBER['id']) 
    match_name_lower = member_name_in_folder.lower()
    
    # ✅ 检查今天和昨天的日期字符串
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    date_strs_to_check = [
        today.strftime("%y%m%d"),
        yesterday.strftime("%y%m%d")
    ]
    
    folders = []
    try:
        for f in parent.iterdir(): 
            if f.is_dir():
                folder_name_lower = f.name.lower()

                # 检查文件夹名称是否包含成员的英文名 且 包含今天或昨天的日期
                is_match = False
                if match_name_lower in folder_name_lower:
                    for date_str in date_strs_to_check:
                        if date_str in folder_name_lower:
                            is_match = True
                            break
                        
                if is_match:
                     folders.append(f)
    except (OSError, PermissionError) as e:  # ✅ 新增这一段
        logging.error(f"遍历录制目录失败: {e}")
        return None
    
    if not folders:
        logging.warning(f"没有找到包含今天/昨天日期和昵称 '{member_name_in_folder}' 的录制文件夹")
        return None
        
    # 返回最新修改时间的文件夹
    return max(folders, key=lambda f: f.stat().st_mtime)

def has_new_ts_files(started_at_unix: int) -> bool:
    """
    ✅ 逻辑修改: 检查最新文件夹中是否有 .ts 文件。
    - 在 GRACEFUL_START_DELAY 之前，无 .ts 文件也返回 True (等待启动)。
    - 在 GRACEFUL_START_DELAY 之后，检查最新 .ts 文件的修改时间是否在 MAX_TS_INACTIVE_TIME 内。
    """
    folder = get_latest_subfolder(TS_PARENT_DIR)
    if folder is None:  # ✅ 尽早检查
        logging.warning("没有找到任何录制子文件夹")
        current_time = time.time()
        time_since_start = current_time - started_at_unix
        return time_since_start < GRACEFUL_START_DELAY  # 宽限期内放行

    current_time = time.time()
    time_since_start = current_time - started_at_unix
    is_graceful_period = time_since_start < GRACEFUL_START_DELAY
    
    if folder is None:
        logging.warning("没有找到任何录制子文件夹")
        # 优雅启动期内放行
        return is_graceful_period

    try:
        ts_files = list(folder.glob("*.ts"))
    except (OSError, PermissionError) as e:
        logging.error(f"读取 TS 文件列表失败: {e}")
        return False
    
    # 场景 1: 没有 .ts 文件
    if not ts_files:
        if is_graceful_period:
            logging.info(f"文件夹 {folder.name} 暂无 .ts 文件，在 GRACEFUL_START_DELAY 内，等待启动...")
            return True
        else:
            # 超过优雅启动期，仍无文件，判断为录制失败
            logging.warning(f"文件夹 {folder.name} 中没有任何 .ts 文件，且已超过 GRACEFUL_START_DELAY")
            return False

    # 场景 2: 有 .ts 文件
    
    # 原有的 .txt 停止标记仍然是有效的停止信号
    try:
        txt_files = list(folder.glob("*.txt"))
    except (OSError, PermissionError):
        txt_files = []
    
    if txt_files:
        logging.warning(f"检测到录制停止标志 .txt 文件在 {folder.name} 中")
        return False
    try:    
        latest_ts = max(ts_files, key=lambda f: f.stat().st_mtime)
        latest_mtime = latest_ts.stat().st_mtime
    except (FileNotFoundError, OSError) as e:
        logging.warning(f"获取文件修改时间失败（文件可能被删除）: {e}")
        return False

    # 核心判断：检查最新文件修改时间是否在 MAX_TS_INACTIVE_TIME 范围内 (60 秒)
    time_since_last_write = current_time - latest_mtime
    
    if time_since_last_write < MAX_TS_INACTIVE_TIME:
        logging.info(f"录制正常: 最新 .ts 文件 {latest_ts.name}，更新时间: {time.ctime(latest_mtime)}，间隔 {time_since_last_write:.0f} 秒")
        return True
    else:
        logging.warning(f"录制停止: 最近的 .ts 文件 {latest_ts.name} (更新于 {time.ctime(latest_mtime)}) 太久，已 {time_since_last_write:.0f} 秒未更新，超过 {MAX_TS_INACTIVE_TIME} 秒")
        return False

def restart_service(service_name):
    """重启服务"""
    global last_restart_time
    
    current_time = time.time()
    time_since_last = current_time - last_restart_time
    
    if time_since_last < MIN_RESTART_INTERVAL:
        wait_time = MIN_RESTART_INTERVAL - time_since_last
        logging.info(f"距离上次重启仅 {time_since_last:.0f} 秒，等待 {wait_time:.0f} 秒后再重启")
        return False
    
    logging.warning(f"执行重启服务: {service_name}")
    result = os.system(f"sudo systemctl restart {service_name}")
    
    if result == 0:
        logging.info(f"服务 {service_name} 重启成功")
        last_restart_time = current_time
        return True
    else:
        logging.error(f"服务 {service_name} 重启失败，返回码: {result}")
        return False

def restart_loop():
    logging.info(f"开始监控重启状态 (成员: {MEMBER['id']})...")
    
    while True:
        is_live, started_at = read_live_status()
        
        if is_live and started_at:
            # ✅ 新增: 计算开播时长
            current_time = time.time()
            time_since_start = current_time - started_at
            
            # ✅ 新增: 如果开播时间太短,跳过检查,等待流稳定
            if time_since_start < GRACEFUL_START_DELAY:
                logging.info(f"{MEMBER['id']} 开播仅 {time_since_start:.1f} 秒,等待流稳定(需 {GRACEFUL_START_DELAY} 秒)")
                time.sleep(RESTART_CHECK_INTERVAL)
                continue
            
            # 开播时间已足够,开始正常检查
            logging.info(f"{MEMBER['id']} 正在直播中 (已开播 {time_since_start:.1f} 秒),检查录制状态...")
            
            if not has_new_ts_files(started_at):
                logging.warning("直播中但未检测到新 ts 文件或录制停止")
                restart_service(SERVICE_NAME)
            else:
                logging.info("录制正常")
        else:
            logging.debug(f"{MEMBER['id']} 当前未直播")
        
        time.sleep(RESTART_CHECK_INTERVAL)

if __name__ == "__main__":    
    if not TS_PARENT_DIR.exists():
        logging.error(f"错误: ts 目录 {TS_PARENT_DIR} 不存在")
        # 即使目录不存在，我们也要确保连接被关闭
        if 'GLOBAL_CONN' in globals() and GLOBAL_CONN:
            GLOBAL_CONN.close()
        sys.exit(1)
    
    try:
        restart_loop()
    except KeyboardInterrupt:
        logging.info("监控循环被用户中断停止。")
    except Exception as e:
        logging.critical(f"监控循环发生严重异常: {e}")
    finally:
        if 'GLOBAL_CONN' in globals() and GLOBAL_CONN:
            try:
                GLOBAL_CONN.close()
                logging.info("数据库持久连接已关闭。")
            except Exception as close_e:
                logging.error(f"关闭数据库连接失败: {close_e}")
        sys.exit(0)