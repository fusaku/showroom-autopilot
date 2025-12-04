import os
import time
import logging
import sys
import cx_Oracle
import subprocess
import signal
from pathlib import Path
from datetime import datetime, timedelta # ✅ 新增导入 timedelta
from logger_config import setup_logger
from config import (
    ENABLED_MEMBERS, 
    RESTART_CHECK_INTERVAL, 
    MIN_RESTART_INTERVAL,
    TS_PARENT_DIR,
    LOG_DIR,
    WALLET_DIR,
    DB_USER,
    DB_PASSWORD,
    DB_TABLE,
    TNS_ALIAS
)

os.environ["TNS_ADMIN"] = WALLET_DIR

"""获取Oracle数据库连接"""
try:
    GLOBAL_CONN = cx_Oracle.connect(user=DB_USER, password=DB_PASSWORD, dsn=TNS_ALIAS)
except Exception as e:
    logging.error(f"Oracle数据库连接失败，脚本退出: {e}")
    sys.exit(1)

# 存储所有成员的进程和状态
member_processes = {}  # {member_id: {'process': subprocess, 'last_live': timestamp, 'last_restart': timestamp}}

# 配置
STOP_DELAY = 300  # 直播结束后5分钟再终止进程
CHECK_INTERVAL = 1  # 每1秒检查一次所有成员
# 【新增】文件活跃性检查宽限期（秒），启动后需等待这么久才检查文件是否生成
# ✅ 此常量 (60秒) 同时作为文件不活动触发重启的阈值
FILE_CHECK_GRACE_PERIOD = 180

# 清理状态标志
is_cleaning_up = False

def read_all_live_status():
    """从数据库读取所有成员的直播状态"""
    global GLOBAL_CONN # 必须声明 GLOBAL_CONN 为 global 才能对其赋值
    
    # 检查连接是否有效，如果为 None (被 cleanup) 或断开，尝试重新连接
    if GLOBAL_CONN is None:
        try:
            logging.warning("全局数据库连接为 None，尝试重新连接...")
            GLOBAL_CONN = cx_Oracle.connect(user=DB_USER, password=DB_PASSWORD, dsn=TNS_ALIAS)
            logging.info("数据库重新连接成功")
        except Exception as e:
            logging.error(f"重新连接数据库失败，跳过状态读取: {e}")
            return {}

    conn = GLOBAL_CONN
    
    try:
        with conn.cursor() as cursor:
            # 只查询 enabled 的成员，排除已经用 systemd 服务管理的成员
            member_ids = [m['id'] for m in ENABLED_MEMBERS if m['id'] != 'hashimoto_haruna']
            placeholders = ','.join([f':id{i}' for i in range(len(member_ids))])
            
            query = f"""
                SELECT MEMBER_ID, IS_LIVE, STARTED_AT
                FROM {DB_TABLE}
                WHERE MEMBER_ID IN ({placeholders})
            """
            
            # 构建绑定参数字典
            bind_params = {f'id{i}': mid for i, mid in enumerate(member_ids)}
            cursor.execute(query, bind_params)
            results = cursor.fetchall()
            
            # 转换为字典格式
            status_dict = {}
            for row in results:
                member_id = row[0]
                is_live = bool(row[1])
                started_at = None
                
                if is_live and row[2]:
                    if isinstance(row[2], datetime):
                        started_at = int(row[2].timestamp())
                    else:
                        try:
                            started_at = int(row[2])
                        except (TypeError, ValueError):
                            logging.error(f"{member_id} STARTED_AT 字段错误: {row[2]}")
                
                status_dict[member_id] = {
                    'is_live': is_live,
                    'started_at': started_at
                }
            
            return status_dict
            
    except Exception as e:
        logging.error(f"从数据库读取状态失败: {e}")
        return {}

# showroom-smart-start.py

def get_latest_subfolder(member_id: str):
    """
    获取指定成员的最新子文件夹。
    通过匹配日期和英文名所有部分（忽略队伍或其他额外信息）。
    ✅ 跨日修改: 检查今天和昨天的日期字符串，以支持跨日直播
    """
    
    # 1. 查找成员的配置信息，获取其英文名 (name_en 字段)
    member_data = next((m for m in ENABLED_MEMBERS if m['id'] == member_id), None)
    if not member_data:
        # 这是一个错误情况，但通常在主循环开始前就能发现
        return None
        
    member_name_en = member_data.get('name_en', member_id) 

    # 【核心修改】
    # 将英文名分割成单词部分，并转为小写
    name_parts_lower = member_name_en.lower().split()
    
    # ✅ 检查今天和昨天的日期字符串
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    date_strs_to_check = [
        today.strftime("%y%m%d"),
        yesterday.strftime("%y%m%d")
    ]
    
    folders = []
    
    # 遍历录制文件的父目录下的所有内容
    try:
        for f in TS_PARENT_DIR.iterdir(): 
            if f.is_dir():
                folder_name_lower = f.name.lower()
                
                # 1. 检查是否包含英文名中的所有单词部分
                if all(part in folder_name_lower for part in name_parts_lower):
                    # 2. 检查是否包含今天或昨天的日期
                    is_date_match = any(date_str in folder_name_lower for date_str in date_strs_to_check)
                    
                    if is_date_match:
                         folders.append(f)
    except Exception as e:
        # 处理可能的权限或路径错误
        logging.error(f"遍历录制目录 {TS_PARENT_DIR} 时出错: {e}")
        return None
                 
    if not folders:
        # 如果未找到任何匹配的文件夹，返回 None
        logging.warning(f"没有找到包含今天/昨天日期和昵称 '{member_name_en}' 的录制文件夹")
        return None
        
    # 返回最新修改时间（st_mtime）的文件夹
    return max(folders, key=lambda f: f.stat().st_mtime)

def has_new_ts_files(member_id: str, started_at_unix: int) -> bool:
    """
    ✅ 逻辑修改: 检查最新文件夹中是否有 .ts 文件，并使用 FILE_CHECK_GRACE_PERIOD 作为不活动阈值。
    """
    folder = get_latest_subfolder(member_id)
    current_time = time.time()
    
    if folder is None:
        logging.debug(f"{member_id}: 没有找到任何录制子文件夹")
        return False
    
    try:
        ts_files = list(folder.glob("*.ts"))
    except (OSError, PermissionError) as e:
        logging.error(f"{member_id}: 读取 TS 文件列表失败: {e}")
        return False

    if not ts_files:
        logging.debug(f"{member_id}: 文件夹 {folder.name} 中没有任何 .ts 文件")
        return False

    try:
        txt_files = list(folder.glob("*.txt"))
    except (OSError, PermissionError):
        txt_files = []
    
    if txt_files:  # 有 .txt 文件说明录制已结束
        logging.warning(f"{member_id}: 检测到录制停止标志 .txt 文件在 {folder.name} 中")
        return False

    try:
        latest_ts = max(ts_files, key=lambda f: f.stat().st_mtime)
        latest_mtime = latest_ts.stat().st_mtime
    except (FileNotFoundError, OSError) as e:
        logging.warning(f"{member_id}: 获取文件修改时间失败: {e}")
        return False
    time_since_last_write = current_time - latest_mtime
    
    # ✅ 核心判断：检查最新文件修改时间是否在 FILE_CHECK_GRACE_PERIOD (60秒) 范围内
    if time_since_last_write < FILE_CHECK_GRACE_PERIOD: 
        logging.debug(f"{member_id}: 录制正常，文件 {latest_ts.name} 更新于 {time_since_last_write:.0f}s 前")
        return True
    
    # 文件太旧，认为录制停止/卡死
    logging.warning(f"{member_id}: 最近的 .ts 文件 {latest_ts.name} (更新于 {time.ctime(latest_mtime)}) 已 {time_since_last_write:.0f} 秒未更新，超过 {FILE_CHECK_GRACE_PERIOD} 秒")
    return False # 返回 False 触发重启

def start_recording_process(member_id: str):
    """启动录制进程，不再使用多IP分配逻辑"""
    global member_processes
    current_time = time.time()
    
    # 检查是否在 MIN_RESTART_INTERVAL 内重复启动
    last_restart = member_processes.get(member_id, {}).get('last_restart', 0)
    if current_time - last_restart < MIN_RESTART_INTERVAL:
        logging.warning(f"{member_id}: 尝试启动过于频繁，跳过。上次启动时间: {datetime.fromtimestamp(last_restart).strftime('%H:%M:%S')}")
        return

    # 1. 如果已有进程在运行，先终止
    if member_id in member_processes and member_processes[member_id].get('process'):
        process = member_processes[member_id]['process']
        if process.poll() is None: # 确保进程确实在运行
            logging.warning(f"{member_id}: 终止现有进程 PID {process.pid}")
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            except Exception as e:
                logging.error(f"{member_id}: 终止进程失败 - {e}")
        # 如果进程已经退出，则清理记录
        member_processes[member_id]['process'] = None
        
    # 2. 构造启动命令
    try:
        VENV_ACTIVATE_DIR = "/home/ubuntu/venv"  # ✅ 修正：VENV 根目录路径
        script_path = Path("/home/ubuntu/showroom-48") / "showroom.py" # ✅ 修正：脚本文件路径
        script_arg = member_id.replace('_', ' ').title()
        
        # 2. 构造 Shell 命令：使用 VENV 路径进行激活，使用 script_path 运行脚本
        full_command = f"source {VENV_ACTIVATE_DIR}/bin/activate && python3 -u {str(script_path)} \"{script_arg}\""
        
        command = [
            "/bin/bash",
            "-c",
            full_command
        ]
        
        logging.info(f"{member_id}: 启动录制进程 - {full_command}") 
        
        # # 启动子进程,重定向输出到日志文件      
        log_file = LOG_DIR / "showroom" / f"{member_id}_recording.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        logging.info(f"{member_id}: 子进程输出将重定向到 {log_file}")
        with open(log_file, 'a') as f:
            process = subprocess.Popen(
                command,
                stdout=f,
                stderr=f,
                text=True # 保持 text=True 兼容性
            )

        # 立即检查进程是否启动失败
        if process.poll() is not None:
            # 进程在启动瞬间就退出了（即失败）
            stdout, stderr = process.communicate()
            logging.error(f"{member_id}: 进程启动失败！PID {process.pid} 立即退出，错误信息如下：")
            if stdout:
                logging.error(f"  Stdout:\n{stdout.strip()}")
            if stderr:
                logging.error(f"  Stderr:\n{stderr.strip()}")
            return

        member_processes[member_id] = {
            'process': process,
            'last_live': current_time,
            'last_restart': current_time  # 标记为刚刚重启
        }
        logging.info(f"{member_id}: 进程启动成功，PID {process.pid}")

    except Exception as e:
        logging.error(f"{member_id}: 启动进程时发生致命错误: {e}")

def stop_recording_process(member_id: str):
    """停止录制进程"""
    if member_id not in member_processes:
        return
    
    process = member_processes[member_id].get('process')
    if not process or process.poll() is not None:
        return
    
    logging.info(f"{member_id}: 停止录制进程 PID {process.pid}")
    
    try:
        process.terminate()
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        logging.warning(f"{member_id}: 进程未响应，强制结束")
        process.kill()
    
    member_processes[member_id]['process'] = None

def is_process_running(member_id: str) -> bool:
    """检查进程是否运行"""
    if member_id not in member_processes:
        return False
    
    process = member_processes[member_id].get('process')
    if not process:
        return False
    
    if process.poll() is not None:
        # 进程已退出
        member_processes[member_id]['process'] = None
        return False
    
    return True

def monitor_all_members():
    """监控所有成员"""
    # 排除特定成员
    monitored_members = [m for m in ENABLED_MEMBERS if m['id'] != 'hashimoto_haruna']
    logging.info(f"开始监控 {len(monitored_members)} 个成员（已排除: hashimoto_haruna）")
    logging.info(f"总共 enabled 成员: {len(ENABLED_MEMBERS)} 个")
    
    while True:
        current_time = time.time()
        
        # 获取所有成员的直播状态
        live_status = read_all_live_status()

        for member in monitored_members:
            member_id = member['id']
            status = live_status.get(member_id, {'is_live': False, 'started_at': None})
            is_live = status['is_live']
            started_at = status['started_at']
            
            # 初始化成员记录
            if member_id not in member_processes:
                member_processes[member_id] = {
                    'process': None,
                    'last_live': 0,
                    'last_restart': 0
                }
            
            if is_live and started_at:
                # 成员正在直播
                member_processes[member_id]['last_live'] = current_time
                
                if not is_process_running(member_id):
                    # 进程未运行，启动它
                    logging.info(f"{member_id}: 检测到直播开始，启动录制")
                    start_recording_process(member_id)
                else:
                    # 进程正在运行
                    last_restart = member_processes[member_id].get('last_restart', 0)
                    time_since_restart = current_time - last_restart
                    
                    # 检查是否仍在 FILE_CHECK_GRACE_PERIOD 宽限期内
                    if time_since_restart < FILE_CHECK_GRACE_PERIOD:
                        # 进程刚启动，不检查文件，继续等待
                        logging.debug(f"{member_id}: 进程刚启动 ({time_since_restart:.0f}s)，等待文件生成")
                    
                    elif not has_new_ts_files(member_id, started_at):
                        # 进程运行已超过宽限期 (60秒)，但未检测到新 ts 文件（或文件不活跃），判定为异常并重启
                        logging.warning(f"{member_id}: 直播中但未检测到新 ts 文件或文件不活跃，重启")
                        start_recording_process(member_id)
                    else:
                        logging.debug(f"{member_id}: 录制正常")
            
            else:
                # 成员未在直播
                last_live = member_processes[member_id].get('last_live', 0)
                time_since_live = current_time - last_live
                
                if is_process_running(member_id):
                    # 进程还在运行
                    if last_live > 0 and time_since_live >= STOP_DELAY:
                        # 已经超过延迟时间，停止进程
                        logging.info(f"{member_id}: 直播结束超过 {STOP_DELAY} 秒，停止录制")
                        stop_recording_process(member_id)
                    elif last_live > 0:
                        # 还在延迟期内
                        remaining = STOP_DELAY - time_since_live
                        logging.debug(f"{member_id}: 直播已结束，{remaining:.0f} 秒后停止录制")
                else:
                    # 进程未运行，无需操作
                    logging.debug(f"{member_id}: 未直播，进程未运行")
        
        # 等待下次检查
        time.sleep(CHECK_INTERVAL)

def cleanup():
    """清理所有资源"""
    global is_cleaning_up, GLOBAL_CONN
    
    # 防止重复清理
    if is_cleaning_up:
        return
    is_cleaning_up = True
    
    logging.info("正在清理所有资源...")
    
    # 停止所有进程
    for member_id, info in member_processes.items():
        process = info.get('process')
        if process and process.poll() is None:
            logging.info(f"{member_id}: 终止进程 PID {process.pid}")
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            except Exception as e:
                logging.error(f"{member_id}: 终止进程失败 - {e}")
    
    # 关闭数据库连接
    if GLOBAL_CONN:
        try:
            GLOBAL_CONN.close()
            logging.info("数据库连接已关闭")
        except Exception as e:
            # 如果已经关闭，忽略错误
            if "not connected" not in str(e).lower():
                logging.error(f"关闭数据库连接失败: {e}")
        finally:
            GLOBAL_CONN = None

if __name__ == "__main__":
    setup_logger(LOG_DIR, "smart_start_handler")
    
    if not TS_PARENT_DIR.exists():
        logging.error(f"错误: ts 目录 {TS_PARENT_DIR} 不存在")
        if 'GLOBAL_CONN' in globals() and GLOBAL_CONN:
            GLOBAL_CONN.close()
        sys.exit(1)
    
    # 注册信号处理
    def signal_handler(signum, frame):
        logging.info(f"收到信号 {signum}，准备退出...")
        cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        monitor_all_members()
    except KeyboardInterrupt:
        logging.info("监控被用户中断")
    except Exception as e:
        logging.critical(f"监控发生严重异常: {e}", exc_info=True)
    finally:
        cleanup()
        sys.exit(0)