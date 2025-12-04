import time
import subprocess
import cx_Oracle
import os
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import *
from merger import merge_once
from datetime import datetime
from typing import Optional
from queue import Queue
from threading import Thread

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
                        if DEBUG_MODE:
                            log(f"文件夹 {folder.name} 与前一个文件夹ts时间差 {time_gap:.0f}秒,判定为同一场直播")
                    else:
                        # 时间差太大,说明是新的直播
                        if DEBUG_MODE:
                            log(f"文件夹 {folder.name} 与前一个文件夹ts时间差 {time_gap:.0f}秒,判定为新直播")
                        
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
            if DEBUG_MODE:
                log(f"文件夹 {ts_dir.name} 在 {seconds_since_last_update:.0f} 秒前还有文件更新，可能还在录制中")
            return False
    
    return True

def has_matching_subtitle_file(ts_dir: Path):
    """检查指定文件夹是否有对应的字幕文件，支持自动处理不匹配情况"""
    if not ts_dir:
        return False
    
    folder_name = ts_dir.name
    
    try:
        date_part = folder_name[:6]  # 取前6位作为日期
        # 转换为完整日期格式 250826 -> 2025-08-26
        year = "20" + date_part[:2]
        month = date_part[2:4]
        day = date_part[4:6]
        date_folder = f"{year}-{month}-{day}"
        
        # 构建字幕文件路径
        subtitle_dir = SUBTITLE_ROOT / date_folder / SUBTITLE_SUBPATH
        exact_subtitle = subtitle_dir / f"{folder_name}.ass"
        
        # 首先检查精确匹配
        if exact_subtitle.exists():
            return True
        
        # 如果精确匹配失败,提取人名进行模糊匹配
        # 从文件夹名称中提取人名部分
        # 格式: "日期 Showroom - 团队信息 人名 时间戳"
        try:
            # 分割文件夹名,提取关键部分
            parts = folder_name.split(" - ")
            if len(parts) >= 2:
                # parts[1] 应该是 "AKB48 Team 8 Hashimoto Haruna 064348"
                name_parts = parts[1].split()
                
                # 查找人名:通常是最后两个单词(姓氏 名字),但要排除时间戳
                # 时间戳格式是6位数字
                filtered_parts = [p for p in name_parts if not (p.isdigit() and len(p) == 6)]
                
                # 如果有足够的部分,取最后两个作为人名
                if len(filtered_parts) >= 2:
                    # 姓氏和名字
                    last_name = filtered_parts[-2]
                    first_name = filtered_parts[-1]
                    name_pattern = f"{last_name} {first_name}"
                else:
                    name_pattern = None
            else:
                name_pattern = None
                
        except Exception as e:
            if DEBUG_MODE:
                log(f"解析人名失败: {folder_name}, 错误: {e}")
            name_pattern = None
        
        # 如果成功提取人名,在同一天的字幕文件中查找包含该人名的文件
        if name_pattern and subtitle_dir.exists():
            # 先尝试精确匹配人名
            subtitle_files = list(subtitle_dir.glob(f"{date_part} Showroom*{name_pattern}*.ass"))
            
            # 如果没找到,尝试只用姓氏匹配
            if not subtitle_files and len(filtered_parts) >= 2:
                last_name = filtered_parts[-2]
                subtitle_files = list(subtitle_dir.glob(f"{date_part} Showroom*{last_name}*.ass"))
            
            if subtitle_files:
                # 找到匹配的字幕文件,自动创建软链接
                source_subtitle = subtitle_files[0]  # 取第一个匹配的
                log(f"检测到字幕文件不匹配情况:")
                log(f"  视频文件夹: {folder_name}")
                log(f"  字幕文件: {source_subtitle.name}")
                log(f"  匹配模式: {name_pattern}")
                log(f"  自动创建匹配的字幕文件...")
                
                try:
                    # 创建软链接
                    exact_subtitle.symlink_to(source_subtitle)
                    log(f"✓ 成功创建软链接: {exact_subtitle.name}")
                    return True
                except FileExistsError:
                    # 软链接已存在,直接返回True
                    log(f"✓ 软链接已存在: {exact_subtitle.name}")
                    return True
                except Exception as e:
                    log(f"✗ 创建软链接失败: {e}")
                    return False
        
        return False
        
    except Exception as e:
        if DEBUG_MODE:
            log(f"解析文件夹日期失败: {folder_name}, 错误: {e}")
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

def read_is_live(member_id: str):
    """从数据库读取指定成员的直播状态 (每次操作建立新连接)"""
    
    # 每次调用时，在 try 块内建立和关闭连接，确保连接有效性和资源释放
    try:
        # 使用 'with' 语句保证连接和游标自动关闭
        with cx_Oracle.connect(user=DB_USER, password=DB_PASSWORD, dsn=TNS_ALIAS) as conn:
            with conn.cursor() as cursor:
                # 查询指定成员的状态
                query = f"""
                    SELECT IS_LIVE
                    FROM {DB_TABLE}
                    WHERE MEMBER_ID = :member_id
                """
                
                cursor.execute(query, {'member_id': member_id})
                result = cursor.fetchone()
                
                if result:
                    # IS_LIVE 字段 (1=True, 0=False)
                    is_live = bool(result[0])
                    if VERBOSE_LOGGING:
                        log(f"从数据库读取状态: 成员 {member_id}, is_live={is_live}")
                    return is_live
                else:
                    if VERBOSE_LOGGING:
                        log(f"数据库中未找到成员 {member_id} 的记录")
                    return False
            
    except Exception as e:
        # 捕获连接失败或查询失败的错误
        log(f"从数据库读取状态失败: {e}")
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
        if DEBUG_MODE:
            log(f"解析人名失败: {folder_name}, 错误: {e}")
            
    return None

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
    
    # 获取未检查且稳定的文件
    unchecked_files = get_unchecked_stable_files(ts_dir, checked_files)
    
    if not unchecked_files:
        return
    
    if DEBUG_MODE or VERBOSE_LOGGING:
        log(f"[{base_name}] 发现 {len(unchecked_files)} 个新的稳定文件需要检查")
    
    # 检查新文件
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_ts_file, f): f for f in unchecked_files}
        for future in as_completed(futures):
            ts_file = futures[future]
            valid_file, err_msg = future.result()
            
            # 标记为已检查
            checked_files.add(ts_file)
            
            if valid_file:
                valid_files.append(valid_file)
                if DEBUG_MODE:
                    log(f"[{base_name}] ✓ {ts_file.name}")
            if err_msg:
                log(f"[{base_name}] {err_msg}")
                error_logs.append(err_msg)


def finalize_live_check(ts_dir: Path, checked_files: set, valid_files: list, error_logs: list):
    """直播结束后的最终检查和文件列表生成"""
    base_name = ts_dir.name
    filelist_txt = ts_dir / FILELIST_NAME
    log_file = OUTPUT_DIR / f"{base_name}{LOG_SUFFIX}"
    
    # 检查剩余未检查的文件（包括不稳定的）
    ts_files = list(ts_dir.glob("*.ts"))
    unchecked_files = [f for f in ts_files if f not in checked_files]
    
    if unchecked_files:
        log(f"[{base_name}] 最终检查剩余 {len(unchecked_files)} 个文件")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(check_ts_file, f): f for f in unchecked_files}
            for future in as_completed(futures):
                ts_file = futures[future]
                valid_file, err_msg = future.result()
                
                if valid_file:
                    valid_files.append(valid_file)
                if err_msg:
                    log(f"[{base_name}] {err_msg}")
                    error_logs.append(err_msg)
    
    # 按文件名排序
    valid_files.sort()
    
    # 写 filelist.txt：无论是否有有效文件，都需要创建这个文件作为检查完成的标记
    with open(filelist_txt, "w", encoding="utf-8") as f:
        if valid_files:
            # 如果有有效文件，写入列表
            for vf in valid_files:
                f.write(f"file '{vf.resolve()}'\n")
            log(f"[{base_name}] 检查完成，共 {len(valid_files)} 个有效文件")
            result_success = True
        else:
            # 如果没有有效文件，写入一个标记注释，防止后续循环重复检查
            f.write(f"# No valid .ts files found. Marked as checked at {datetime.now()}\n")
            log(f"[{base_name}] 没有有效的 .ts 文件，已标记为检查完成。")
            result_success = False
    
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
        log(f"[{base_name}] 存在异常/为空，日志写入：{log_file}")
    
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
        if DEBUG_MODE:
            log(f"直播 {base_name} 已检查完成，跳过")
        return True  # 返回True表示该文件夹已完成
    
    # 检查文件数量是否足够开始检查
    if not has_files_to_check(ts_dir):
        if DEBUG_MODE:
            ts_count = len(list(ts_dir.glob("*.ts")))
            log(f"直播 {base_name} 文件数量不足({ts_count}/{MIN_FILES_FOR_CHECK})，等待中...")
        return False  # 返回False表示该文件夹还不能处理
    
    # 直播进行中 - 增量检查稳定的文件
    if current_time - state['last_check'] >= LIVE_CHECK_INTERVAL:
        if VERBOSE_LOGGING:
            log(f"处理中：{base_name}，进行增量检查...")
        check_live_folder_incremental(
            ts_dir, 
            state['checked_files'], 
            state['valid_files'], 
            state['error_logs']
        )
        state['last_check'] = current_time
    else:
        if DEBUG_MODE:
            remaining = LIVE_CHECK_INTERVAL - (current_time - state['last_check'])
            log(f"文件夹 {base_name} 等待 {remaining:.0f} 秒后进行下次检查")
    
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
        if DEBUG_MODE:
            log(f"清理过期文件夹状态: {folder_path.name}")
        del folder_states[folder_path]

def merge_worker():
    """独立的合并工作线程，从队列中串行执行合并任务"""
    log("✨ 合并工作线程已启动")
    
    while True:
        try:
            # 从队列获取任务，阻塞等待
            task = merge_queue.get()
            
            if task is None:  # None 是停止信号
                log("合并工作线程收到停止信号")
                break
            
            group_key, group_folders = task
            
            try:
                log(f"🔄 [合并队列] 开始合并: {group_key}")
                earliest_folder = min(group_folders, key=lambda x: x.stat().st_ctime)
                merged_video = OUTPUT_DIR / f"{earliest_folder.name}{OUTPUT_EXTENSION}"
                
                if not merged_video.exists():
                    merge_once(target_folders=group_folders)
                    log(f"✅ [合并队列] 完成: {group_key}")
                else:
                    log(f"⏭️  [合并队列] 文件已存在，跳过: {group_key}")
                    
            except Exception as e:
                log(f"❌ [合并队列] 失败 {group_key}: {e}")
                import traceback
                log(traceback.format_exc())
            finally:
                merge_queue.task_done()  # 标记任务完成
                
        except Exception as e:
            log(f"合并工作线程异常: {e}")
            time.sleep(1)

# ========================= 主循环 =========================

def main_loop():
    log("开始监控直播文件夹...")
    
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
                            if DEBUG_MODE:
                                log(f"直播组 {group_key} 有 {len(group_folders)} 个文件夹,限制为 {MAX_CONCURRENT_FOLDERS_PER_LIVE} 个")
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
                if DEBUG_MODE:
                    log("未找到直播文件夹,等待中...")
                time.sleep(CHECK_INTERVAL)
                continue
            
            # ==== 直接进入按组处理,不需要全局判断 ====
            for group_key, group_folders in grouped.items():
                member_id = extract_member_name_from_folder(group_folders[0].name)
                
                # 该组的网络状态
                if member_id:
                    group_is_streaming = read_is_live(member_id)
                else:
                    group_is_streaming = False
                    if DEBUG_MODE:
                        log(f"无法提取成员ID: {group_key}")
                
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
                        log(f"字幕文件检查已达到 {subtitle_check_count[group_key]} 次,判定为无字幕视频: {group_key}")
                        group_has_subtitle = True  # 强制通过
                    
                    if group_has_subtitle:
                        group_can_merge = True  # 字幕找到或已强制通过，允许合并
                        log(f"[{group_key}] 满足合并条件 (字幕找到或超时)，开始最终检查。")
                    else:
                        # 仍在等待字幕，计数器未达到 5 次
                        log(f"[{group_key}] 等待字幕文件生成中... (第 {subtitle_check_count[group_key]} 次检查)")

                # 3. 如果满足合并条件 (group_can_merge)
                if group_can_merge:
                    
                    # (A) 最终检查 (调用 finalize_live_check，此时会创建 filelist.txt 标记)
                    for ts_dir in group_folders:
                        if not has_been_merged(ts_dir):  # 再次检查防止重复操作
                            log(f"对已结束的直播进行最终检查: {ts_dir.name}")
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
                            log(f"📋 直播组 {group_key} 已完成检查，加入合并队列 (当前队列: {merge_queue.qsize()} 个任务)")
                            merge_queue.put((group_key, group_folders))
                            submitted_merges.add(group_key)  # 标记为已提交
                        else:
                            log(f"⏭️  直播组 {group_key} 合并文件已存在，跳过")

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
                if DEBUG_MODE:
                    log(f"清理字幕计数器中已完成/不活跃的组: {key}")
                del subtitle_check_count[key]
                # 同时清理已提交的合并记录
                if key in submitted_merges:
                    submitted_merges.discard(key)
            
            time.sleep(CHECK_INTERVAL)
            
    except KeyboardInterrupt:
        log("收到停止信号,等待合并队列完成...")
        merge_queue.join()  # 等待所有合并任务完成
        merge_queue.put(None)  # 发送停止信号
        log("程序退出")
    except Exception as e:
        log(f"主循环发生错误: {e}")
        import traceback
        log(traceback.format_exc())

if __name__ == "__main__":
    main_loop()