import time
import subprocess
import cx_Oracle
import os
import threading
import traceback
import logging
import re
import sys
import hashlib
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from logger_config import setup_logger
setup_logger()
from config import *
from merger import merge_once
from datetime import datetime
from typing import Optional
from queue import Queue
from threading import Thread
from sync_module import syncer

os.environ["TNS_ADMIN"] = WALLET_DIR # 新增

# 在全局变量区域添加
merge_queue = Queue()  # 合并任务队列
merge_lock = threading.Lock()  # 合并锁（可选，Queue本身是线程安全的）

# ========================= 文件夹操作 =========================

def find_all_live_folders(parent_dir: Path):
    """获取所有直播文件夹路径"""
    folders = []
    for f in parent_dir.iterdir():
        if f.is_dir() and not f.name.startswith("temp_"):  # 排除临时目录
            folders.append(f)
    return sorted(folders, key=lambda x: x.stat().st_mtime)


def find_latest_live_folder(parent_dir: Path):
    """获取最新创建的直播文件夹路径（保持向后兼容）"""
    folders = [f for f in parent_dir.iterdir() if f.is_dir()]
    return max(folders, key=lambda x: x.stat().st_mtime, default=None)


def has_been_merged(ts_dir: Path):
    """判断该直播是否已经合并过"""
    return (ts_dir / FILELIST_NAME).exists()


def has_files_to_check(ts_dir: Path):
    """检查文件夹是否有足够的文件可以开始检查"""
    ts_files = list(ts_dir.glob("*.ts"))
    return len(ts_files) >= MIN_FILES_FOR_CHECK


def all_folders_completed(folders):
    """检查所有文件夹是否都已完成检查（都有filelist.txt）"""
    if not folders:
        return False
    return all(has_been_merged(folder) for folder in folders)


# ========================= 文件状态检查 =========================
def group_folders_by_member(folders):
    """将文件夹按成员分组,根据ts文件时间戳判断是否为同一场直播(支持跨日)"""
    from collections import defaultdict
    groups = defaultdict(list)
    
    # 先按成员ID分组
    member_folders = defaultdict(list)
    for folder in folders:
        member_id = extract_member_name_from_folder(folder.name)
        if member_id:
            member_folders[member_id].append(folder)
        else:
            # 解析失败的单独分组
            groups[f"unknown_{folder.name}"].append(folder)
    
    # 对每个成员的文件夹按创建时间排序,然后根据ts文件时间判断是否连续
    for member_id, member_folder_list in member_folders.items():
        # 按文件夹创建时间排序
        member_folder_list.sort(key=lambda x: x.stat().st_ctime)
        
        if not member_folder_list:
            continue
            
        # 用于标记当前直播组
        current_group = []
        group_index = 0
        
        for i, folder in enumerate(member_folder_list):
            if i == 0:
                # 第一个文件夹,直接加入当前组
                current_group.append(folder)
            else:
                # 获取当前文件夹最早的ts文件时间
                current_ts_files = list(folder.glob("*.ts"))
                if not current_ts_files:
                    # 没有ts文件,按文件夹时间判断(降级处理)
                    prev_folder = member_folder_list[i-1]
                    time_diff = folder.stat().st_ctime - prev_folder.stat().st_ctime
                    if time_diff < 14400:  # 4小时
                        current_group.append(folder)
                    else:
                        # 保存当前组并开始新组
                        first_folder = current_group[0]
                        date_part = first_folder.name[:6]
                        key = f"{date_part}_{member_id}_{group_index}"
                        groups[key] = current_group
                        group_index += 1
                        current_group = [folder]
                    continue
                
                current_earliest_ts = min(current_ts_files, key=lambda x: x.stat().st_ctime)
                current_ts_time = current_earliest_ts.stat().st_ctime
                
                # 获取前一个文件夹最晚的ts文件时间
                prev_folder = current_group[-1]  # 用当前组的最后一个文件夹
                prev_ts_files = list(prev_folder.glob("*.ts"))
                
                if prev_ts_files:
                    prev_latest_ts = max(prev_ts_files, key=lambda x: x.stat().st_ctime)
                    prev_ts_time = prev_latest_ts.stat().st_ctime
                    
                    # 计算两个文件夹ts文件的时间差
                    time_gap = current_ts_time - prev_ts_time
                    
                    # 如果时间差小于5分钟(300秒),认为是同一场直播
                    # 正常情况下ts文件每2秒一个,5分钟已经很宽松了
                    if time_gap < 300:
                        current_group.append(folder)
                        logging.debug(f"文件夹 {folder.name} 与前一个文件夹ts时间差 {time_gap:.0f}秒,判定为同一场直播")
                    else:
                        # 时间差太大,说明是新的直播
                        logging.info(f"文件夹 {folder.name} 与前一个文件夹ts时间差 {time_gap:.0f}秒,判定为新直播")
                        
                        # 保存当前组
                        first_folder = current_group[0]
                        date_part = first_folder.name[:6]
                        key = f"{date_part}_{member_id}_{group_index}"
                        groups[key] = current_group
                        
                        # 开始新组
                        group_index += 1
                        current_group = [folder]
                else:
                    # 前一个文件夹没有ts文件,降级到文件夹时间判断
                    time_diff = folder.stat().st_ctime - prev_folder.stat().st_ctime
                    if time_diff < 14400:
                        current_group.append(folder)
                    else:
                        first_folder = current_group[0]
                        date_part = first_folder.name[:6]
                        key = f"{date_part}_{member_id}_{group_index}"
                        groups[key] = current_group
                        group_index += 1
                        current_group = [folder]
        
        # 保存最后一组
        if current_group:
            first_folder = current_group[0]
            date_part = first_folder.name[:6]
            key = f"{date_part}_{member_id}_{group_index}"
            groups[key] = current_group
    
    return groups

def has_matching_subtitle_for_group(group_folders):
    """检查一组文件夹(同一个直播)是否有对应的字幕文件
    
    只需要检查组内最早的文件夹,因为字幕是按直播生成的,不是按文件夹
    """
    if not group_folders:
        return False
    
    # 取最早的文件夹作为代表
    earliest_folder = min(group_folders, key=lambda x: x.stat().st_ctime)
    return has_matching_subtitle_file(earliest_folder)

def is_file_stable(file_path: Path, stable_time: int = FILE_STABLE_TIME):
    """检查文件是否稳定（在指定时间内没有被修改）"""
    if not file_path.exists():
        return False
    time_since_modified = time.time() - file_path.stat().st_mtime
    return time_since_modified > stable_time


def is_live_active(ts_dir: Path):
    """检查直播是否还在进行中"""
    ts_files = list(ts_dir.glob("*.ts"))
    if not ts_files:
        return False
    
    latest_mtime = max(f.stat().st_mtime for f in ts_files)
    seconds_since_last_update = time.time() - latest_mtime
    return seconds_since_last_update <= LIVE_INACTIVE_THRESHOLD


def is_really_stream_ended(all_folders, grace_period=FINAL_INACTIVE_THRESHOLD):
    """综合判断直播是否真正结束 - 检查所有文件夹的文件活跃度"""
    current_time = time.time()
    
    for ts_dir in all_folders:
        ts_files = list(ts_dir.glob("*.ts"))
        if not ts_files:
            continue
            
        # 获取该文件夹最新文件的修改时间
        latest_mtime = max(f.stat().st_mtime for f in ts_files)
        seconds_since_last_update = current_time - latest_mtime
        
        # 如果任何文件夹的文件在宽限期内还有更新，说明可能还在录制
        if seconds_since_last_update <= grace_period:
            logging.debug(f"文件夹 {ts_dir.name} 在 {seconds_since_last_update:.0f} 秒前还有文件更新，可能还在录制中")
            return False
    
    return True

def has_matching_subtitle_file(ts_dir: Path):
    """
    改进版：检查是否有匹配的字幕文件
    支持：成员名带空格、日文无空格、时间戳允许±2分钟误差
    """
    folder_name = ts_dir.name
    
    # 1. 使用正则精准拆分文件夹名
    # 格式假设：YYMMDD Showroom - 名字 123456
    # (\d{6}) -> 日期
    # Showroom\s+-\s+ -> 固定前缀
    # (.+?) -> 名字（贪婪匹配，直到遇到最后那个数字前的空格）
    # \s+(\d{6})$ -> 结尾的时间戳数字
    pattern = r'^(\d{6})\s+Showroom\s+-\s+(.+?)\s+(\d{6})$'
    match = re.match(pattern, folder_name)
    
    if not match:
        logging.warning(f"文件夹格式不标准，无法解析: {folder_name}")
        return False

    v_date = match.group(1)      # 视频日期
    v_name = match.group(2).strip() # 成员名（不论中英日）
    v_time = int(match.group(3)) # 视频时间戳（转成数字方便计算）
    logging.debug(f"解析成功：日期={v_date}, 名字={v_name}, 时间戳={v_time}")

    # 2. 遍历字幕目录
    if not SUBTITLES_SOURCE_ROOT.exists():
        return False

    best_match_sub = None
    min_diff = 999999 # 初始设为一个很大的秒数

    # 扫描所有comments.json
    for sub_file in SUBTITLES_SOURCE_ROOT.rglob("*comments.json"):
        sub_name = sub_file.stem
        
        # 匹配规则：字幕文件名里必须包含日期和成员名
        if v_date in sub_name and v_name in sub_name:
            # 尝试从字幕文件名提取时间戳数字
            sub_time_match = re.search(r'(\d{6})', sub_name.replace(v_date, "", 1)) # 排除掉日期后的第一个6位数字
            
            if sub_time_match:
                s_time = int(sub_time_match.group(1))
                diff = abs(v_time - s_time) # 计算时间差
                
                # 如果时间差在 60 秒（1分钟）以内，且是目前最接近的
                if diff < 60 and diff < min_diff:
                    min_diff = diff
                    best_match_sub = sub_file

    if best_match_sub:
        logging.info(f"✅ 成功匹配字幕: {best_match_sub.name} (时间误差: {min_diff}秒)")
        return True

    return False

def get_earliest_active_folder(all_folders):
    """获取最早的活跃文件夹（当前录制中且有文件的文件夹中最早创建的）"""
    active_folders = []
    for folder in all_folders:
        ts_files = list(folder.glob("*.ts"))
        # 必须同时满足：有文件 + 还在录制中（文件还在活跃）
        if ts_files and is_live_active(folder):
            active_folders.append(folder)
    
    if not active_folders:
        return None
    
    # 返回创建时间最早的文件夹
    return min(active_folders, key=lambda x: x.stat().st_ctime)

# ========================= 网络状态检查 (数据库) =========================
db_pool = None

def get_db_pool():
    """获取或初始化连接池（单例模式）"""
    global db_pool
    if db_pool is None:
        try:
            db_pool = cx_Oracle.SessionPool(
                user=DB_USER,
                password=DB_PASSWORD,
                dsn=TNS_ALIAS,
                min=0,          # 不使用时保持 0 个连接，节省资源
                max=10,          # 20人直播时，10个连接足以应对
                increment=1,
                threaded=True,  # 支持多线程安全
                getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT
            )
            logging.info("✨ 数据库连接池已初始化 (按需分配)")
        except Exception as e:
            logging.error(f"❌ 初始化连接池失败: {e}")
    return db_pool


def read_is_live(member_id: str):
    """从连接池中获取连接执行查询，执行完自动归还"""
    pool = get_db_pool()
    if not pool:
        return False
    
    try:
        # 使用 with pool.acquire() 会在执行完毕后自动将连接放回池中，而不是关闭它
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                query = f"SELECT IS_LIVE FROM {DB_TABLE} WHERE MEMBER_ID = :member_id"
                cursor.execute(query, {'member_id': member_id})
                result = cursor.fetchone()
                
                if result:
                    is_live = bool(result[0])
                    
                    logging.info(f"数据库状态: {member_id} is_live={is_live}")
                    return is_live
                return False
    except Exception as e:
        logging.error(f"查询成员 {member_id} 状态失败 (数据库可能未开启): {e}")
        return False

def extract_member_name_from_folder(folder_name: str) -> Optional[str]:
    """从文件夹名称中提取人名部分，用于模糊匹配数据库中的 member_id"""
    try:
        # 文件夹格式: "日期 Showroom - 团队信息 人名 时间戳"
        parts = folder_name.split(" - ")
        if len(parts) >= 2:
            # parts[1] 应该是 "AKB48 Team 8 Hashimoto Haruna 233156"
            name_parts = parts[1].split()
            
            # 过滤掉时间戳 (6位数字)
            filtered_parts = [p for p in name_parts if not (p.isdigit() and len(p) == 6)]
            
            # 通常人名是最后两个单词 (姓 名字)
            if len(filtered_parts) >= 2:
                # 拼接成 "hashimoto_haruna" 格式 (注意数据库中的格式)
                last_name = filtered_parts[-2].lower()
                first_name = filtered_parts[-1].lower()
                
                # 尝试使用 "姓_名" 格式匹配数据库ID
                return f"{last_name}_{first_name}"
                
            # 如果只有一个人名部分，则返回该部分
            elif len(filtered_parts) == 1:
                return filtered_parts[-1].lower()

    except Exception as e:
        logging.error(f"解析人名失败: {folder_name}, 错误: {e}")
            
    return None

# ========================= 全局跨文件夹去重器 =========================
class TSDeduplicator:
    def __init__(self):
        # 格式: { member_id: { "md5_size": timestamp } }
        self.fingerprints = {}  
        self.ttl = 43200  # 改为 12 小时（完全足够覆盖同一场长直播的任何断线重连）
        self._insert_count = 0  # 新增：独立的全局插入计数器

    def check_and_add(self, ts_file: Path) -> bool:
        """检查文件是否重复。返回 True 表示重复，False 表示是新文件"""
        folder_name = ts_file.parent.name
        member_id = extract_member_name_from_folder(folder_name) or "unknown"
        
        if member_id not in self.fingerprints:
            self.fingerprints[member_id] = {}

        # 优化：使用独立计数器，严格每检查 1000 个文件（约 16 分钟）执行一次全局过期清理
        self._insert_count += 1
        if self._insert_count % 1000 == 0:
            current_time = time.time()
            for m_id in list(self.fingerprints.keys()):
                self.fingerprints[m_id] = {
                    k: v for k, v in self.fingerprints[m_id].items() 
                    if current_time - v < self.ttl
                }

        # 计算哈希指纹（读取前 512KB 足以精确区分）
        fsize = ts_file.stat().st_size
        hasher = hashlib.md5()
        try:
            with open(ts_file, 'rb') as f:
                hasher.update(f.read(524288))
            fingerprint = f"{hasher.hexdigest()}_{fsize}"
        except Exception:
            fingerprint = f"{ts_file.name}_{fsize}" # 降级

        # 判断并记录
        if fingerprint in self.fingerprints[member_id]:
            return True  # 拦截！是重复回放
        else:
            self.fingerprints[member_id][fingerprint] = time.time()
            return False # 放行！是新文件

# 实例化全局去重器
global_deduplicator = TSDeduplicator()

# ========================= 文件检查和处理 =========================

def check_ts_file(ts_file: Path):
    """检测ts文件是否含视频和音频流"""
    # 构建FFprobe命令，使用配置的参数
    base_cmd = ["ffprobe"]
    
    # 添加隐藏banner选项
    if FFMPEG_HIDE_BANNER:
        base_cmd.append("-hide_banner")
    
    # 添加日志级别
    base_cmd.extend(["-v", FFMPEG_LOGLEVEL])
    
    v_cmd = base_cmd + [
        "-select_streams", "v",
        "-show_entries", "stream=index",
        "-of", "csv=p=0",
        str(ts_file)
    ]
    a_cmd = base_cmd + [
        "-select_streams", "a",
        "-show_entries", "stream=index",
        "-of", "csv=p=0",
        str(ts_file)
    ]
    
    try:
        video_stream = subprocess.run(
            v_cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True, 
            timeout=FFPROBE_TIMEOUT
        ).stdout.strip()
        
        audio_stream = subprocess.run(
            a_cmd, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True, 
            timeout=FFPROBE_TIMEOUT
        ).stdout.strip()
        
        if video_stream and audio_stream:
            return ts_file, None
        else:
            msg = f"[不同步或缺流] {ts_file.name}"
            return None, msg
    except Exception as e:
        return None, f"[错误] {ts_file.name} 检测失败: {e}"


def get_unchecked_stable_files(ts_dir: Path, checked_files: set):
    """获取未检查且稳定的ts文件"""
    ts_files = list(ts_dir.glob("*.ts"))
    unchecked_files = []
    
    for ts_file in ts_files:
        # 如果文件还没检查过且已经稳定
        if ts_file not in checked_files and is_file_stable(ts_file):
            unchecked_files.append(ts_file)
    
    return unchecked_files


def check_live_folder_incremental(ts_dir: Path, checked_files: set, valid_files: list, error_logs: list):
    """增量检查直播文件夹中的新文件"""
    base_name = ts_dir.name

    # <---【修改点2A】新增：提前解析 Member ID，用于传给同步模块做判断
    current_member_id = extract_member_name_from_folder(base_name)
    
    # 获取未检查且稳定的文件
    unchecked_files = get_unchecked_stable_files(ts_dir, checked_files)
    
    if not unchecked_files:
        return
    logging.debug(f"[{base_name}] 发现 {len(unchecked_files)} 个新的稳定文件需要检查")
    
    # 检查新文件
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_ts_file, f): f for f in unchecked_files}
        for future in as_completed(futures):
            ts_file = futures[future]
            valid_file, err_msg = future.result()
            
            # 标记为已检查
            checked_files.add(ts_file)
            
            if valid_file:
                # === 新增：调用全局去重器 ===
                if global_deduplicator.check_and_add(ts_file):
                    logging.warning(f"[{base_name}] 拦截跨文件夹重复片段: {ts_file.name}")
                else:
                    valid_files.append(valid_file)
                    syncer.sync_to_4c(valid_file, member_id=current_member_id)
                    logging.debug(f"[{base_name}] ✓ {ts_file.name}")
                # ===========================
            if err_msg:
                logging.error(f"[{base_name}] {err_msg}")
                error_logs.append(err_msg)


def finalize_live_check(ts_dir: Path, checked_files: set, valid_files: list, error_logs: list):
    """直播结束后的最终检查和文件列表生成"""
    base_name = ts_dir.name
    filelist_txt = ts_dir / FILELIST_NAME
    log_file = OUTPUT_DIR / f"{base_name}{LOG_SUFFIX}"
    
    # 检查剩余未检查的文件（包括不稳定的）
    ts_files = list(ts_dir.glob("*.ts"))
    unchecked_files = [f for f in ts_files if f not in checked_files]
    
    #【获取 Member ID 用于同步】
    current_member_id = extract_member_name_from_folder(base_name)
    if unchecked_files:
        logging.debug(f"[{base_name}] 最终检查剩余 {len(unchecked_files)} 个文件")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(check_ts_file, f): f for f in unchecked_files}
            for future in as_completed(futures):
                ts_file = futures[future]
                valid_file, err_msg = future.result()
                
                if valid_file:
                    # === 新增：调用全局去重器 ===
                    if global_deduplicator.check_and_add(ts_file):
                        logging.warning(f"[{base_name}] 最终检查拦截重复: {ts_file.name}")
                    else:
                        valid_files.append(valid_file)
                        syncer.sync_to_4c(valid_file, member_id=current_member_id)
                    # ===========================
                if err_msg:
                    logging.error(f"[{base_name}] {err_msg}")
                    error_logs.append(err_msg)
    
    # 按文件名排序
    valid_files.sort(key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f.name)])
    
    # 写 filelist.txt：无论是否有有效文件，都需要创建这个文件作为检查完成的标记
    with open(filelist_txt, "w", encoding="utf-8") as f:
        if valid_files:
            # 如果有有效文件，写入列表
            for vf in valid_files:
                f.write(f"file '{vf.resolve()}'\n")
            logging.debug(f"[{base_name}] 检查完成，共 {len(valid_files)} 个有效文件")
            result_success = True
        else:
            # 如果没有有效文件，写入一个标记注释，防止后续循环重复检查
            f.write(f"# No valid .ts files found. Marked as checked at {datetime.now()}\n")
            logging.debug(f"[{base_name}] 没有有效的 .ts 文件，已标记为检查完成。")
            result_success = False
    
    # 目的：把刚才生成的 filelist.txt 传给 4C，作为“结束信号”
    try:
        # 1. 解析成员ID
        member_id = extract_member_name_from_folder(ts_dir.name)
        
        # 2. 发送信号
        # 新代码：直接调用审计方法，因为它就是 txt 文件
        syncer.sync_filelist_and_audit(filelist_txt, member_id=member_id)
        
        logging.info(f"📡 [信号发送] 已同步 filelist.txt (带审计) 到 4C: {ts_dir.name}")
    except Exception as e:
        logging.error(f"❌ 同步 filelist.txt 失败: {e}")
    # ================= 同步字幕 =================
    try:
        syncer.sync_subtitles()
        logging.info("📡 触发字幕同步")
    except Exception as e:
        logging.error(f"❌ 字幕同步异常: {e}")
    
    # 写日志文件
    if error_logs or not result_success: # 如果有错误或结果失败，都写日志
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with open(log_file, "w", encoding="utf-8") as logf:
            logf.write(f"检测时间：{datetime.now()}\n")
            logf.write(f"总文件数：{len(ts_files)}\n")
            logf.write(f"有效文件数：{len(valid_files)}\n")
            logf.write(f"错误文件数：{len(error_logs)}\n\n")
            if not result_success:
                 logf.write("此文件夹中未找到任何有效的视频流文件，已强制标记完成。\n\n")
            logf.write("\n".join(error_logs))
        logging.error(f"[{base_name}] 存在异常/为空，日志写入：{log_file}")
    
    # 返回的是检查是否找到了有效文件
    return result_success

# ========================= 文件夹处理逻辑 =========================

def process_single_folder(ts_dir: Path, folder_states: dict, all_folders: list, current_time: float):
    """处理单个文件夹的检查逻辑"""
    base_name = ts_dir.name
    
    # 初始化文件夹状态
    if ts_dir not in folder_states:
        folder_states[ts_dir] = {
            'checked_files': set(),
            'valid_files': [],
            'error_logs': [],
            'last_check': 0,
            'creation_time': current_time
        }
    
    state = folder_states[ts_dir]
    
    # 检查是否已经完成检查
    if has_been_merged(ts_dir):
        logging.debug(f"直播 {base_name} 已检查完成，跳过")
        return True  # 返回True表示该文件夹已完成
    
    # 检查文件数量是否足够开始检查
    if not has_files_to_check(ts_dir):
        ts_count = len(list(ts_dir.glob("*.ts")))
        logging.debug(f"直播 {base_name} 文件数量不足({ts_count}/{MIN_FILES_FOR_CHECK})，等待中...")
        return False  # 返回False表示该文件夹还不能处理
    
    # 直播进行中 - 增量检查稳定的文件
    if current_time - state['last_check'] >= LIVE_CHECK_INTERVAL:
        
        logging.debug(f"处理中：{base_name}，进行增量检查...")
        check_live_folder_incremental(
            ts_dir, 
            state['checked_files'], 
            state['valid_files'], 
            state['error_logs']
        )
        state['last_check'] = current_time
    else:
        remaining = LIVE_CHECK_INTERVAL - (current_time - state['last_check'])
        logging.debug(f"文件夹 {base_name} 等待 {remaining:.0f} 秒后进行下次检查")
    
    return False  # 直播还在进行中，文件夹未完成


def cleanup_old_folder_states(folder_states: dict, active_folders: list, current_time: float):
    """清理过期的文件夹状态，释放内存"""
    folders_to_remove = []
    
    for folder_path, state in folder_states.items():
        # 如果文件夹不在活动列表中，且状态保留时间超过配置的延迟
        if (folder_path not in active_folders and 
            current_time - state.get('last_check', 0) > FOLDER_CLEANUP_DELAY):
            folders_to_remove.append(folder_path)
        # 如果文件夹已经有filelist.txt，强制清理
        elif has_been_merged(folder_path):
            folders_to_remove.append(folder_path)
    
    for folder_path in folders_to_remove:
        logging.debug(f"清理过期文件夹状态: {folder_path.name}")
        del folder_states[folder_path]

def merge_worker():
    """独立的合并工作线程，从队列中串行执行合并任务"""
    logging.info("✨ 合并工作线程已启动")
    
    while True:
        try:
            # 从队列获取任务，阻塞等待
            task = merge_queue.get()
            
            if task is None:  # None 是停止信号
                logging.info("合并工作线程收到停止信号")
                break
            
            group_key, group_folders = task
            
            try:
                logging.info(f"🔄 [合并队列] 开始合并: {group_key}")
                earliest_folder = min(group_folders, key=lambda x: x.stat().st_ctime)
                merged_video = OUTPUT_DIR / f"{earliest_folder.name}{OUTPUT_EXTENSION}"
                
                if not merged_video.exists():
                    merge_once(target_folders=group_folders)
                    logging.info(f"✅ [合并队列] 完成: {group_key}")
                else:
                    logging.warning(f"⏭️  [合并队列] 文件已存在，跳过: {group_key}")
                    
            except Exception as e:
                logging.error(f"❌ [合并队列] 失败 {group_key}: {e}")
                logging.error(traceback.format_exc())
            finally:
                merge_queue.task_done()  # 标记任务完成
                
        except Exception as e:
            logging.error(f"合并工作线程异常: {e}")
            time.sleep(1)

# ========================= 主循环 =========================

def main_loop():
    logging.info("开始监控直播文件夹...")
    
    # 启动合并工作线程
    merge_thread = Thread(target=merge_worker, daemon=True, name="MergeWorker")
    merge_thread.start()
    
    folder_states = {}
    subtitle_check_count = {}
    submitted_merges = set()  # 添加这行：追踪已提交到队列的组
    
    try:
        while True:
            current_time = time.time()
            
            # 获取直播文件夹
            if PROCESS_ALL_FOLDERS:
                all_folders = find_all_live_folders(PARENT_DIR)
                all_folders = [f for f in all_folders if not has_been_merged(f)]

                # 按直播分组后再限制每组的文件夹数量
                if all_folders:
                    # 先按成员和时间分组
                    grouped = group_folders_by_member(all_folders)

                    # 对每组限制文件夹数量(保留最早的文件夹)
                    all_folders = []  # ← 清空准备重建
                    for group_key, group_folders in list(grouped.items()):
                        # 按创建时间排序(最早的在前)
                        group_folders.sort(key=lambda x: x.stat().st_ctime)

                        # 如果该组超过限制,只取最早的N个
                        if len(group_folders) > MAX_CONCURRENT_FOLDERS_PER_LIVE:
                            logging.debug(f"直播组 {group_key} 有 {len(group_folders)} 个文件夹,限制为 {MAX_CONCURRENT_FOLDERS_PER_LIVE} 个")
                            group_folders = group_folders[:MAX_CONCURRENT_FOLDERS_PER_LIVE]
                            grouped[group_key] = group_folders  # ← 更新 grouped 字典

                        all_folders.extend(group_folders)  # ← 重建 all_folders
                else:
                    grouped = {}  # 空字典
            else:
                latest_folder = find_latest_live_folder(PARENT_DIR)
                if latest_folder and not has_been_merged(latest_folder):
                    all_folders = [latest_folder]
                    grouped = group_folders_by_member(all_folders)
                else:
                    all_folders = []
                    grouped = {}  # 空字典

            if not all_folders:
                logging.debug("未找到直播文件夹,等待中...")
                time.sleep(CHECK_INTERVAL)
                continue
            
            # ==== 直接进入按组处理,不需要全局判断 ====
            for group_key, group_folders in grouped.items():
                member_id = extract_member_name_from_folder(group_folders[0].name)
                
                # 该组的网络状态
                group_is_streaming = read_is_live(member_id) if member_id else False
                
                # 该组的文件活跃度
                group_files_active = not is_really_stream_ended(group_folders, FINAL_INACTIVE_THRESHOLD)
                
                # --- 新的字幕检查和合并逻辑 ---
                
                # 1. 跳过已经完成合并的组
                group_is_merged = all(has_been_merged(f) for f in group_folders)
                if group_is_merged:
                    continue  # 跳过该组，处理下一个

                group_can_merge = False  # 标记该组是否可以进入最终检查/合并流程
                
                # 2. 如果直播结束且文件已稳定 (触发最终检查/合并的条件)
                if not group_is_streaming and not group_files_active:
                    
                    # 开始字幕检查计数和强制通过逻辑 (不再依赖 has_been_merged)
                    if group_key not in subtitle_check_count:
                        subtitle_check_count[group_key] = 0
                        
                    subtitle_check_count[group_key] += 1
                    group_has_subtitle = has_matching_subtitle_for_group(group_folders)
                    
                    # 【强制退出等待】字幕未找到，但检查次数达到 5 次
                    if not group_has_subtitle and subtitle_check_count[group_key] >= 5:
                        logging.warning(f"字幕文件检查已达到 {subtitle_check_count[group_key]} 次,判定为无字幕视频: {group_key}")
                        group_has_subtitle = True  # 强制通过
                    
                    if group_has_subtitle:
                        group_can_merge = True  # 字幕找到或已强制通过，允许合并
                        logging.info(f"[{group_key}] 满足合并条件 (字幕找到或超时)，开始最终检查。")
                    else:
                        # 仍在等待字幕，计数器未达到 5 次
                        logging.warning(f"[{group_key}] 等待字幕文件生成中... (第 {subtitle_check_count[group_key]} 次检查)")

                # 3. 如果满足合并条件 (group_can_merge)
                if group_can_merge:
                    
                    # (A) 最终检查 (调用 finalize_live_check，此时会创建 filelist.txt 标记)
                    for ts_dir in group_folders:
                        if not has_been_merged(ts_dir):  # 再次检查防止重复操作
                            logging.info(f"对已结束的直播进行最终检查: {ts_dir.name}")
                            # 确保 folder_states 中有该文件夹的状态
                            if ts_dir not in folder_states:
                                folder_states[ts_dir] = {'checked_files': set(), 'valid_files': [], 'error_logs': []}
                            
                            finalize_live_check(
                                ts_dir,
                                folder_states[ts_dir]['checked_files'],
                                folder_states[ts_dir]['valid_files'],
                                folder_states[ts_dir]['error_logs']
                            )
                    # (B) 合并该组 - 提交到合并队列
                    if all(has_been_merged(f) for f in group_folders):
                        earliest_folder = min(group_folders, key=lambda x: x.stat().st_ctime)
                        merged_video = OUTPUT_DIR / f"{earliest_folder.name}{OUTPUT_EXTENSION}"

                        if not merged_video.exists():
                            logging.info(f"📋 直播组 {group_key} 已完成检查，加入合并队列 (当前队列: {merge_queue.qsize()} 个任务)")
                            merge_queue.put((group_key, group_folders))
                            submitted_merges.add(group_key)  # 标记为已提交
                        else:
                            logging.warning(f"⏭️  直播组 {group_key} 合并文件已存在，跳过")

                # 4. 如果仍在直播/文件活跃，则继续执行增量检查
                elif group_is_streaming or group_files_active:
                    for ts_dir in group_folders:
                        if has_files_to_check(ts_dir) and not has_been_merged(ts_dir):
                            # 直接调用 process_single_folder，让它自己管理 folder_states 字典中的状态
                            process_single_folder(ts_dir, folder_states, all_folders, current_time)
            
            # 清理过期状态
            cleanup_old_folder_states(folder_states, all_folders, current_time)
            
            # 清理字幕检查计数器
            active_group_keys = set(grouped.keys())

            # 找出不再活跃的 group_key 进行清理
            keys_to_remove = [key for key in subtitle_check_count.keys() 
                              if key not in active_group_keys]
            
            for key in keys_to_remove:
                logging.debug(f"清理字幕计数器中已完成/不活跃的组: {key}")
                del subtitle_check_count[key]
                # 同时清理已提交的合并记录
                if key in submitted_merges:
                    submitted_merges.discard(key)
            
            time.sleep(CHECK_INTERVAL)
            
    except KeyboardInterrupt:
        logging.warning("收到停止信号，正在清理资源...")
        if db_pool:
            try:
                db_pool.close() # 显式关闭连接池，释放所有数据库会话
                logging.info("数据库连接池已安全关闭")
            except:
                pass
        merge_queue.join()
        logging.info("程序退出")
    except Exception as e:
        logging.error(f"主循环发生错误: {e}")
        logging.error(traceback.format_exc())
    finally: 
        if db_pool:
            try:
                db_pool.close()
                logging.info("数据库连接池已关闭")
            except:
                pass

if __name__ == "__main__":
    main_loop()