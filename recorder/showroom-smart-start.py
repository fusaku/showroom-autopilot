#!/usr/bin/env python3
"""
Showroom 智能启动服务（最终版）
监控数据库中的直播状态,自动启动录制服务

核心特性：
1. 进程驱动而非配置驱动（每轮只扫描1次系统进程）
2. 自动检测并杀掉重复进程（保留最老的）
3. 防止多实例运行（文件锁机制）
4. 接管进程：10秒缓冲期，异常则杀掉不重启
5. 自己启动的进程：35秒宽限期，异常则立即重启
6. 无启动频率限制：快速响应直播开始和进程异常
7. 性能提升 100+ 倍（从7分钟降到2秒）
"""

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))

# ============================================================
# 防止多实例运行
# ============================================================
import fcntl

LOCK_FILE = Path("/tmp/showroom-smart-start.lock")
lock_fd = None

def acquire_lock():
    """获取文件锁，防止多实例运行"""
    global lock_fd
    try:
        lock_fd = open(LOCK_FILE, 'w')
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return True
    except IOError:
        print(f"❌ 错误：另一个 {Path(__file__).name} 实例正在运行")
        print(f"   如果确认没有其他实例，请删除锁文件：{LOCK_FILE}")
        return False

def release_lock():
    """释放文件锁"""
    global lock_fd
    if lock_fd:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()
            if LOCK_FILE.exists():
                LOCK_FILE.unlink()
        except Exception:
            pass

# 在导入其他模块前先获取锁
if not acquire_lock():
    sys.exit(1)

# ============================================================
# 初始化日志系统 (必须在导入config之前)
# ============================================================
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
sys.path.insert(0, str(_project_root))

from logger_config import setup_logger
setup_logger()

# ============================================================
# 导入依赖
# ============================================================
import time
import logging
import cx_Oracle
import subprocess
import signal
import psutil
from datetime import datetime, timedelta
from config import *

# ============================================================
# 全局变量
# ============================================================
os.environ["TNS_ADMIN"] = WALLET_DIR
GLOBAL_CONN = None

# ✅ 新增：获取录制器实例ID（必须）
INSTANCE_ID = os.environ.get('INSTANCE_ID')

if not INSTANCE_ID:
    logging.critical("❌ 错误: 未设置环境变量 INSTANCE_ID")
    logging.critical("   录制器必须指定实例ID，例如:")
    logging.critical("   INSTANCE_ID=recorder-a python showroom-smart-start.py")
    release_lock()
    sys.exit(1)

logging.info(f"🎯 录制器实例: {INSTANCE_ID}")

GLOBAL_CONN = get_db_connection()
if not GLOBAL_CONN:
    logging.critical("首次数据库连接失败,脚本退出。")
    release_lock()
    sys.exit(1)

# 存储所有成员的进程和状态
member_processes = {}  # {member_id: {'process': subprocess, 'last_live': timestamp, ...}}

# 清理状态标志
is_cleaning_up = False

def read_all_live_status():
    """
    从数据库读取所有直播状态。
    ✅ 修改：只返回分配给本录制器实例的成员
    """
    global GLOBAL_CONN
    
    MAX_ATTEMPTS = 2
    
    for attempt in range(MAX_ATTEMPTS):
        if GLOBAL_CONN is None:
            logging.warning(f"全局数据库连接为空，尝试重新连接 (第 {attempt + 1} 次)...")
            GLOBAL_CONN = get_db_connection()
            if not GLOBAL_CONN:
                if attempt == MAX_ATTEMPTS - 1:
                    logging.error("多次尝试重连数据库失败，返回空状态。")
                    return {}
                time.sleep(1)
                continue
        
        conn = GLOBAL_CONN
        
        try:
            with conn.cursor() as cursor:
                # ✅ 修改：JOIN 查询，只返回分配给本实例的成员
                query = f"""
                    SELECT 
                        ls.MEMBER_ID, 
                        ls.IS_LIVE, 
                        ls.STARTED_AT
                    FROM {DB_TABLE} ls
                    JOIN ADMIN.MEMBERS m ON ls.MEMBER_ID = m.MEMBER_ID
                    JOIN ADMIN.MEMBER_INSTANCES mi ON m.ID = mi.MEMBER_ID
                    WHERE ls.IS_LIVE = 1
                      AND m.ENABLED = 1
                      -- 这个脚本暂时不控制橋本陽菜配信的录制
                      AND m.MEMBER_ID != 'hashimoto_haruna'
                      AND mi.INSTANCE_ID = :instance_id
                      AND mi.INSTANCE_TYPE = 'recorder'
                      AND mi.ENABLED = 1
                """
                
                cursor.execute(query, {'instance_id': INSTANCE_ID})
                results = cursor.fetchall()
                
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
                
                # ✅ 新增：日志输出当前负责的成员
                if results:
                    live_count = sum(1 for v in status_dict.values() if v['is_live'])
                    logging.debug(f"[{INSTANCE_ID}] 当前负责 {len(status_dict)} 个成员，{live_count} 个在直播")
                
                return status_dict
                
        except cx_Oracle.Error as e:
            logging.error(f"从数据库读取状态失败（连接可能失效）: {e}")
            GLOBAL_CONN = None
            
            if attempt == MAX_ATTEMPTS - 1:
                logging.error("多次尝试读取数据库状态失败，返回空状态。")
                return {}
            
            time.sleep(1)
            continue
            
        except Exception as e:
            logging.error(f"读取状态时发生非数据库异常: {e}")
            return {}
            
    return {}

def get_latest_subfolder(member_id: str):
    """
    获取指定成员的最新子文件夹。
    检查今天和昨天的日期字符串，以支持跨日直播
    """
    member_data = next((m for m in ENABLED_MEMBERS if m['id'] == member_id), None)
    if not member_data:
        return None
        
    member_name_en = member_data.get('name_en', member_id) 
    name_parts_lower = member_name_en.lower().split()
    
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    date_strs_to_check = [
        today.strftime("%y%m%d"),
        yesterday.strftime("%y%m%d")
    ]
    
    folders = []
    
    try:
        for f in TS_PARENT_DIR.iterdir(): 
            if f.is_dir():
                folder_name_lower = f.name.lower()
                
                if all(part in folder_name_lower for part in name_parts_lower):
                    is_date_match = any(date_str in folder_name_lower for date_str in date_strs_to_check)
                    
                    if is_date_match:
                         folders.append(f)
    except Exception as e:
        logging.error(f"遍历录制目录 {TS_PARENT_DIR} 时出错: {e}")
        return None
                 
    if not folders:
        logging.warning(f"没有找到包含今天/昨天日期和昵称 '{member_name_en}' 的录制文件夹")
        return None
        
    return max(folders, key=lambda f: f.stat().st_mtime)

def has_new_ts_files(member_id: str, started_at_unix: int) -> bool:
    """
    检查最新文件夹中是否有 .ts 文件，并使用 FILE_INACTIVITY_THRESHOLD 作为不活动阈值。
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
    
    if txt_files:
        logging.warning(f"{member_id}: 检测到录制停止标志 .txt 文件在 {folder.name} 中")
        return False

    try:
        latest_ts = max(ts_files, key=lambda f: f.stat().st_mtime)
        latest_mtime = latest_ts.stat().st_mtime
    except (FileNotFoundError, OSError) as e:
        logging.warning(f"{member_id}: 获取文件修改时间失败: {e}")
        return False
    time_since_last_write = current_time - latest_mtime
    
    if time_since_last_write < FILE_INACTIVITY_THRESHOLD: 
        logging.debug(f"{member_id}: 录制正常，文件 {latest_ts.name} 更新于 {time_since_last_write:.0f}s 前")
        return True
    
    logging.warning(
        f"{member_id}: 最近的 .ts 文件 {latest_ts.name} "
        f"(更新于 {datetime.fromtimestamp(latest_mtime).strftime('%a %b %d %H:%M:%S %Y')}) "
        f"已 {time_since_last_write:.0f} 秒未更新，超过 {FILE_INACTIVITY_THRESHOLD} 秒"
    )
    return False

def start_recording_process(member_id: str):
    """启动录制进程（无频率限制）"""
    current_time = time.time()
    
    member_data = next((m for m in ENABLED_MEMBERS if m['id'] == member_id), None)
    if not member_data:
        logging.error(f"{member_id}: 在配置中未找到成员信息，无法启动录制")
        return
    
    member_name_en = member_data.get('name_en', member_id)
    cmd_str = f'source {VENV_ACTIVATE_DIR}/bin/activate && python3 -u {SHOWROOM_SCRIPT_PATH} "{member_name_en}"'
    
    log_file_name = f"{member_id}_recording.log"
    log_file_path = LOG_DIR / "showroom" / log_file_name

    try:
        log_fd = open(log_file_path, 'a')
        
        logging.info(f"{member_id}: 启动录制进程 - {cmd_str}")
        logging.info(f"{member_id}: 子进程输出将重定向到 {log_file_path}")

        process = subprocess.Popen(
            ["bash", "-c", cmd_str],
            stdout=log_fd,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setpgrp
        )

        log_fd.close()

        time.sleep(0.3)
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            logging.error(f"{member_id}: 进程启动失败！PID {process.pid} 立即退出")
            if stdout:
                logging.error(f"  Stdout: {stdout.strip()}")
            if stderr:
                logging.error(f"  Stderr: {stderr.strip()}")
            return

        member_processes[member_id] = {
            'process': process,
            'pid': process.pid,
            'last_live': current_time,
            'last_restart': current_time,
            'is_adopted': False,
            'adopted_time': None
        }
        logging.info(f"{member_id}: 进程启动成功，PID {process.pid}")

    except Exception as e:
        logging.error(f"{member_id}: 启动进程时发生致命错误: {e}")

def is_alive_process(p) -> bool:
    """兼容检查 subprocess.Popen 和 psutil.Process 是否还在运行"""
    if p is None: return False
    try:
        if hasattr(p, 'poll'):
            return p.poll() is None
        return p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return False

def stop_recording_process(member_id: str, graceful: bool = True):
    """停止录制进程（直接kill）"""
    if member_id not in member_processes:
        return
    
    process = member_processes[member_id].get('process')
    
    if not is_alive_process(process):
        member_processes[member_id]['process'] = None
        return
    
    try:
        pid = process.pid if hasattr(process, 'pid') else "Unknown"
        
        logging.info(f"{member_id}: 直接终止进程 PID {pid}")
        process.kill()
        
        try:
            if isinstance(process, psutil.Process):
                process.wait(timeout=3)
            else:
                process.wait(timeout=3)
        except:
            pass
                
    except Exception as e:
        logging.error(f"{member_id}: 停止进程时出错: {e}")
    finally:
        member_processes[member_id]['process'] = None
        member_processes[member_id]['pid'] = None

# ============================================================
# 核心重构：进程驱动的监控逻辑
# ============================================================

def scan_all_showroom_processes():
    """
    扫描系统中所有 showroom.py 进程
    返回: {member_id: [psutil.Process, ...]} 映射（支持检测重复进程）
    """
    process_map = {}
    search_path = "showroom.py"
    
    for proc in psutil.process_iter(['pid', 'cmdline', 'create_time']):
        try:
            cmdline = proc.info.get('cmdline') or []
            
            for i, arg in enumerate(cmdline):
                if search_path in arg and i + 1 < len(cmdline):
                    member_name = cmdline[i + 1]  # "Shinohara Kyoka"
                    member_id = member_name.lower().replace(' ', '_')
                    
                    if member_id not in process_map:
                        process_map[member_id] = []
                    process_map[member_id].append(proc)
                    break
                    
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    return process_map


def kill_duplicate_processes(member_id: str, processes: list):
    """
    处理重复进程：保留最老的，杀掉其他的
    
    Returns:
        保留的进程对象
    """
    if len(processes) <= 1:
        return processes[0] if processes else None
    
    # 按创建时间排序，最老的在前
    try:
        processes_sorted = sorted(processes, key=lambda p: p.create_time())
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        processes_sorted = sorted(processes, key=lambda p: p.pid)
    
    keep_process = processes_sorted[0]
    duplicate_processes = processes_sorted[1:]
    
    logging.warning(f"{member_id}: 发现 {len(duplicate_processes)} 个重复进程，保留 PID {keep_process.pid}")
    
    for proc in duplicate_processes:
        try:
            logging.warning(f"{member_id}: 终止重复进程 PID {proc.pid}")
            proc.kill()
            proc.wait(timeout=3)
        except (psutil.NoSuchProcess, psutil.TimeoutExpired):
            pass
        except Exception as e:
            logging.error(f"{member_id}: 终止重复进程 PID {proc.pid} 失败: {e}")
    
    return keep_process


def handle_running_process(member_id: str, proc: psutil.Process, live_status: dict):
    """
    处理正在运行的进程
    
    两种情况：
    1. 接管的进程（is_adopted=True）：10秒缓冲期，异常则杀掉不重启
    2. 自己启动的进程（is_adopted=False）：35秒宽限期，异常则立即重启
    """
    current_time = time.time()
    
    # 初始化成员记录
    if member_id not in member_processes:
        member_processes[member_id] = {
            'process': None,
            'pid': None,
            'last_live': current_time,
            'last_restart': 0,
            'is_adopted': False,
            'adopted_time': None
        }
    
    info = member_processes[member_id]
    
    # 检查是否需要接管进程（用 PID 比较）
    is_new_process = (
        info['process'] is None or 
        info.get('pid') != proc.pid
    )
    if is_new_process:
        logging.info(f"{member_id}: 发现并接管遗留进程 PID {proc.pid}")
        info['process'] = proc
        info['pid'] = proc.pid
        info['last_live'] = current_time
        info['adopted_time'] = current_time  # 接管时间
        info['is_adopted'] = True
        return
    
    # 获取直播状态
    status = live_status.get(member_id, {'is_live': False, 'started_at': None})
    is_live = status['is_live']
    started_at = status['started_at']
    
    # ============================================================
    # 情况1: 直播进行中
    # ============================================================
    if is_live and started_at:
        info['last_live'] = current_time
        
        # ========== 子情况A: 接管的进程（遗留进程） ==========
        if info.get('is_adopted', False):
            ADOPTED_GRACE_PERIOD = 10  # 接管进程专用缓冲期
            time_since_adopted = current_time - info.get('adopted_time', current_time)

            if time_since_adopted < ADOPTED_GRACE_PERIOD:
                logging.debug(f"{member_id}: [接管进程] 等待缓冲期 ({time_since_adopted:.0f}s / {ADOPTED_GRACE_PERIOD}s)")
                return

            # 检查文件...
            if not has_new_ts_files(member_id, started_at):
                logging.warning(f"{member_id}: [接管进程] 文件异常，直接终止进程（不重启，等待下次直播）")
                stop_recording_process(member_id, graceful=False)
                return
            else:
                logging.info(f"{member_id}: [接管进程] 录制正常，解除接管标记，转为正常监控")
                info['is_adopted'] = False
                info['last_restart'] = current_time  # ← 解除接管时，设置为"现在启动的"
                info.pop('adopted_time', None)  # ← 删除接管时间（不再需要）
                return
        
        # ========== 子情况B: 自己启动的进程（正常监控） ==========
        time_since_restart = current_time - info.get('last_restart', 0)
        
        # 35秒宽限期内不检查
        if time_since_restart < FILE_CHECK_GRACE_PERIOD:
            logging.debug(f"{member_id}: 进程启动中 ({time_since_restart:.0f}s / {FILE_CHECK_GRACE_PERIOD}s)，等待文件生成")
            return
        
        # 宽限期后检查文件，异常则立即重启（不等3次）
        if not has_new_ts_files(member_id, started_at):
            logging.warning(f"{member_id}: 未检测到有效录制流，执行强制重启")
            stop_recording_process(member_id, graceful=False)
            start_recording_process(member_id)
        else:
            logging.debug(f"{member_id}: 录制正常")
    
    # ============================================================
    # 情况2: 直播未进行
    # ============================================================
    else:
        time_since_live = current_time - info.get('last_live', current_time)
        
        # 区分接管进程和已确认正常的进程
        if info.get('is_adopted', False):
            # 接管的进程：只等30秒
            if time_since_live >= 30:
                logging.info(f"{member_id}: [接管进程] 直播未进行超过30秒，直接终止")
                stop_recording_process(member_id, graceful=True)
            else:
                remaining = 30 - time_since_live
                logging.debug(f"{member_id}: [接管进程] 直播未进行，观察中 ({time_since_live:.0f}s / 30s)")
        else:
            # 已解除接管的进程或自己启动的进程：等300秒
            if time_since_live >= STOP_DELAY:
                logging.info(f"{member_id}: 满足停止条件 (已等待 {time_since_live:.0f}s)，停止录制")
                stop_recording_process(member_id, graceful=True)
            else:
                remaining = STOP_DELAY - time_since_live
                logging.debug(f"{member_id}: 直播未进行，将在 {remaining:.0f}s 后停止进程")


def monitor_all_members():
    """主监控循环（最终版）"""
    monitored_members = [m for m in ENABLED_MEMBERS if m['id'] != 'hashimoto_haruna']
    logging.info(f"开始监控 {len(monitored_members)} 个成员（已排除: hashimoto_haruna）")
    logging.info(f"总共 enabled 成员: {len(ENABLED_MEMBERS)} 个")
    logging.info(f"🚀 录制器启动: {INSTANCE_ID}")
    logging.info(f"📊 只处理分配给本实例的成员（由检测器动态分配）")
    logging.info(f"🔍 每 {RESTART_CHECK_INTERVAL} 秒检查一次录制状态")
    
    while True:
        loop_start = time.time()
        
        # # 阶段1：重新加载成员配置
        # try:
        #     all_enabled = get_enabled_members()
        #     monitored_members = [m for m in all_enabled if m['id'] != 'hashimoto_haruna']
        # except Exception as e:
        #     logging.error(f"重新加载成员配置失败: {e}，继续使用旧配置")
        
        # 阶段2：扫描系统进程（只扫描1次！）
        system_processes = scan_all_showroom_processes()
        
        if system_processes:
            total_processes = sum(len(procs) for procs in system_processes.values())
            logging.debug(f"发现 {total_processes} 个 showroom.py 进程，分属 {len(system_processes)} 个成员")
        
        # 阶段3：批量查询直播状态（只查询1次！）
        live_status = read_all_live_status()
        
        # 阶段4：处理已存在的进程
        handled_members = set()
        
        for member_id, processes in system_processes.items():
            # 跳过不在监控列表中的成员
            if not any(m['id'] == member_id for m in monitored_members):
                logging.debug(f"{member_id}: 不在监控列表中，跳过")
                continue
            
            # 处理重复进程
            if len(processes) > 1:
                proc = kill_duplicate_processes(member_id, processes)
            else:
                proc = processes[0]
            
            # 检查进程是否仍然存活
            try:
                if not proc.is_running():
                    logging.debug(f"{member_id}: 进程 PID {proc.pid} 已退出")
                    if member_id in member_processes:
                        member_processes[member_id]['process'] = None
                    continue
            except psutil.NoSuchProcess:
                continue
            
            # 处理进程
            handle_running_process(member_id, proc, live_status)
            handled_members.add(member_id)
        
        # 阶段5：检查是否需要启动新进程（无频率限制）
        for member in monitored_members:
            member_id = member['id']
            
            # 如果已经有进程在运行，跳过
            if member_id in handled_members:
                continue
            
            # 获取直播状态
            status = live_status.get(member_id, {'is_live': False, 'started_at': None})
            is_live = status['is_live']
            started_at = status['started_at']
            
            # 初始化成员记录
            if member_id not in member_processes:
                member_processes[member_id] = {
                    'process': None,
                    'pid': None,
                    'last_live': time.time(),
                    'last_restart': 0,
                    'is_adopted': False,
                    'adopted_time': None
                }
            
            # 如果正在直播但没有进程，立即启动（无频率限制）
            if is_live and started_at:
                member_processes[member_id]['last_live'] = time.time()
                logging.info(f"{member_id}: 检测到直播开始，启动录制")
                start_recording_process(member_id)
        
        # 性能统计
        loop_duration = time.time() - loop_start
        if loop_duration > 5:
            logging.warning(f"⚠️  本轮循环耗时 {loop_duration:.1f}s（超过5秒）")
        else:
            logging.debug(f"本轮循环耗时 {loop_duration:.2f}s")
        
        # 等待下次检查
        time.sleep(RESTART_CHECK_INTERVAL)


def cleanup():
    """清理资源：释放文件锁和关闭数据库连接"""
    global is_cleaning_up, GLOBAL_CONN
    
    if is_cleaning_up:
        return
    is_cleaning_up = True
    
    logging.info("主脚本正在关闭，保持录制进程在后台运行...")
    
    # 释放文件锁
    release_lock()
    
    # 关闭数据库连接
    if GLOBAL_CONN:
        try:
            GLOBAL_CONN.close()
            logging.info("数据库连接已关闭")
        except Exception as e:
            if "not connected" not in str(e).lower():
                logging.error(f"关闭数据库连接失败: {e}")
        finally:
            GLOBAL_CONN = None

if __name__ == "__main__":    
    if not TS_PARENT_DIR.exists():
        logging.error(f"错误: ts 目录 {TS_PARENT_DIR} 不存在")
        if 'GLOBAL_CONN' in globals() and GLOBAL_CONN:
            GLOBAL_CONN.close()
        release_lock()
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