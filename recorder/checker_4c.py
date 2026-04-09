# recorder/checker_4c.py

import time
import subprocess
import os
import threading
import traceback
import logging
import re
import sys
from pathlib import Path
from queue import Queue
from threading import Thread

# ================= 路径与环境设置 =================
# 确保能引用 shared 和当前目录下的模块
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))
from logger_config import setup_logger
setup_logger()
from config import * # 复用 OUTPUT_DIR, SUBTITLES_SOURCE_ROOT 等配置
from upscaler import upscale_file  # 需确保 recorder/upscaler.py 存在
from merger import merge_once      # 复用现有的合并模块


# 全局任务队列
merge_queue = Queue()

# ========================= 逻辑复用区 =========================

def group_folders_by_member(folders):
    """
    【逻辑复用】将文件夹按成员分组
    因为 rsync 保留了时间戳，4C 的分组结果将与 3C 完全一致
    """
    from collections import defaultdict
    groups = defaultdict(list)
    member_folders = defaultdict(list)
    
    for folder in folders:
        member_id = extract_member_name_from_folder(folder.name)
        if member_id:
            member_folders[member_id].append(folder)
        else:
            groups[f"unknown_{folder.name}"].append(folder)
    
    for member_id, member_folder_list in member_folders.items():
        member_folder_list.sort(key=lambda x: x.stat().st_ctime)
        if not member_folder_list: continue
            
        current_group = []
        group_index = 0
        
        for i, folder in enumerate(member_folder_list):
            if i == 0:
                current_group.append(folder)
            else:
                prev_folder = member_folder_list[i-1]
                # 简单的时间差判定 (4小时)，与 3C 逻辑保持兼容
                time_diff = folder.stat().st_ctime - prev_folder.stat().st_ctime
                if time_diff < 14400: 
                    current_group.append(folder)
                else:
                    first_folder = current_group[0]
                    date_part = first_folder.name[:6]
                    key = f"{date_part}_{member_id}_{group_index}_{int(first_folder.stat().st_ctime)}"
                    groups[key] = current_group
                    group_index += 1
                    current_group = [folder]
        
        if current_group:
            first_folder = current_group[0]
            date_part = first_folder.name[:6]
            key = f"{date_part}_{member_id}_{group_index}_{int(first_folder.stat().st_ctime)}"
            groups[key] = current_group
    
    return groups

def extract_member_name_from_folder(folder_name: str):
    """【逻辑复用】提取 Member ID"""
    try:
        parts = folder_name.split(" - ")
        if len(parts) >= 2:
            name_parts = parts[1].split()
            filtered_parts = [p for p in name_parts if not (p.isdigit() and len(p) == 6)]
            if len(filtered_parts) >= 2:
                return f"{filtered_parts[-2].lower()}_{filtered_parts[-1].lower()}"
            elif len(filtered_parts) == 1:
                return filtered_parts[-1].lower()
    except: pass
    return None

def has_matching_subtitle_for_group(group_folders):
    """【逻辑复用】检查字幕是否存在 (复用 config 里的 SUBTITLES_SOURCE_ROOT)"""
    # 4C 上 SUBTITLES_SOURCE_ROOT 指向同步过来的字幕目录
    if not group_folders or not SUBTITLES_SOURCE_ROOT.exists():
        return False
    
    earliest_folder = min(group_folders, key=lambda x: x.stat().st_ctime)
    folder_name = earliest_folder.name
    
    pattern = r'^(\d{6})\s+Showroom\s+-\s+(.+?)\s+(\d{6})$'
    match = re.match(pattern, folder_name)
    if not match: return False

    v_date = match.group(1)
    v_name = match.group(2).strip()
    v_time = int(match.group(3))

    # 扫描 config 中配置的字幕目录
    for sub_file in SUBTITLES_SOURCE_ROOT.rglob("*comments.json"):
        sub_name = sub_file.stem
        if v_date in sub_name and v_name in sub_name:
            sub_time_match = re.search(r'(\d{6})', sub_name.replace(v_date, "", 1))
            if sub_time_match:
                s_time = int(sub_time_match.group(1))
                diff = abs(v_time - s_time)
                if diff < 120: # 允许2分钟误差
                    return True
    return False

# ========================= 4C 核心处理 =========================

def process_live_folder_upscale(incoming_folder: Path, processed_folder: Path):
    """
    核心任务：将 Incoming (360p) 的文件拉伸到 Processed (1080p)
    """
    if not incoming_folder.exists():
        return

    processed_folder.mkdir(parents=True, exist_ok=True)
    src_files = sorted(list(incoming_folder.glob("*.ts")))
    
    # 找出 Processed 目录缺失的文件
    pending_files = []
    for src in src_files:
        dst = processed_folder / src.name
        # 如果目标不存在或大小为0，则加入任务
        if not dst.exists() or dst.stat().st_size == 0:
            pending_files.append(src)

    if not pending_files:
        return

    logging.info(f"⚡ [{incoming_folder.name}] 新增 {len(pending_files)} 个分片，开始拉伸...")
    
    # 串行执行拉伸 (ffmpeg 耗资源，不建议并行)
    for src in pending_files:
        dst = processed_folder / src.name
        upscale_file(src, dst) # 调用 upscaler 模块

def check_group_ready_to_merge(group_folders):
    """
    判断条件：
    1. 3C 信号：Incoming 文件夹内有 filelist.txt
    2. 拉伸完成：Processed 文件数 >= Incoming 文件数
    """
    for folder in group_folders:
        # 1. 检查 3C 结束信号
        signal_file = folder / FILELIST_NAME # filelist.txt
        if not signal_file.exists():
            return False, f"等待 3C 同步信号: {folder.name}"

        # 2. 检查拉伸进度
        proc_folder = PROCESSED_DIR / folder.name
        if not proc_folder.exists():
             return False, f"等待创建拉伸目录: {proc_folder.name}"
             
        src_count = len(list(folder.glob("*.ts")))
        dst_count = len(list(proc_folder.glob("*.ts")))
        
        if dst_count < src_count:
            return False, f"拉伸进行中: {dst_count}/{src_count}"
            
    return True, "Ready"

def finalize_upscale_group(group_folders):
    """
    收尾工作：在 Processed 文件夹中生成 filelist.txt
    这样 merger 模块才能识别并合并它们
    """
    for folder in group_folders:
        processed_dir = PROCESSED_DIR / folder.name
        
        # 确保目录存在
        if not processed_dir.exists(): continue

        # 生成 filelist.txt (merger 模块依赖这个)
        ts_files = sorted(list(processed_dir.glob("*.ts")), key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f.name)])
        filelist_txt = processed_dir / FILELIST_NAME # 使用 config 定义的文件名
        
        with open(filelist_txt, "w", encoding="utf-8") as f:
            for ts in ts_files:
                f.write(f"file '{ts.resolve()}'\n")

# ========================= 合并线程 =========================

def merge_worker():
    """
    合并线程：直接复用 merger 模块
    """
    logging.info("✨ 合并工作线程已启动")
    
    while True:
        try:
            task = merge_queue.get()
            if task is None: break
            
            group_key, processed_group_folders = task
            
            logging.info(f"🔄 [合并队列] 启动: {group_key}")
            
            # 【复用】调用 merger.py 的核心函数
            # 注意：传入的是 1080p 的文件夹路径列表
            try:
                merge_once(target_folders=processed_group_folders)
                logging.info(f"✅ [合并队列] 完成: {group_key}")
            except Exception as e:
                logging.error(f"❌ [合并队列] 失败 {group_key}: {e}")
                logging.error(traceback.format_exc())
            
        except Exception as e:
            logging.error(f"合并线程异常: {e}")
        finally:
            merge_queue.task_done()

# ========================= 主循环 =========================

def main_loop():
    logging.info("🚀 4C 拉伸检查服务启动...")
    
    # 目录初始化
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    if not OUTPUT_DIR.exists():
        logging.warning(f"输出目录不存在，将自动创建: {OUTPUT_DIR}")
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 启动合并线程
    merge_thread = Thread(target=merge_worker, daemon=True, name="MergeWorker")
    merge_thread.start()
    
    submitted_merges = set()
    subtitle_check_count = {}

    while True:
        try:
            # 1. 扫描 Incoming
            if not INCOMING_DIR.exists():
                time.sleep(5)
                continue

            # 排除 temp 文件夹
            all_folders = [f for f in INCOMING_DIR.iterdir() 
                          if f.is_dir() and not f.name.startswith("temp_")]
            
            if not all_folders:
                time.sleep(CHECK_INTERVAL)
                continue

            # 2. 分组 (与 3C 逻辑一致)
            grouped = group_folders_by_member(all_folders)
            
            # 3. 逐组处理
            for group_key, group_folders in grouped.items():
                
                # 如果已提交合并，跳过
                if group_key in submitted_merges:
                    continue

                # === 步骤 A: 拉伸 (Incoming -> Processed) ===
                for folder in group_folders:
                    proc_folder = PROCESSED_DIR / folder.name
                    process_live_folder_upscale(folder, proc_folder)

                # === 步骤 B: 检查合并条件 ===
                is_ready, status_msg = check_group_ready_to_merge(group_folders)
                
                if is_ready:
                    # 检查字幕 (本地是否已同步)
                    if group_key not in subtitle_check_count:
                        subtitle_check_count[group_key] = 0
                    
                    has_sub = has_matching_subtitle_for_group(group_folders)
                    
                    # 允许合并的条件：有字幕 OR 等待超时 (5次轮询)
                    if has_sub or subtitle_check_count[group_key] > 5:
                        if not has_sub:
                            logging.warning(f"[{group_key}] 等待字幕超时，强制合并")
                        
                        logging.info(f"📋 [{group_key}] 提交合并任务...")
                        
                        # 1. 给 Processed 文件夹生成 filelist.txt (Merger 需要)
                        finalize_upscale_group(group_folders)
                        
                        # 2. 构造指向 Processed 的路径列表
                        processed_group_folders = [PROCESSED_DIR / f.name for f in group_folders]
                        
                        # 3. 放入队列，交给 merger 模块处理
                        merge_queue.put((group_key, processed_group_folders))
                        submitted_merges.add(group_key)
                        
                    else:
                        subtitle_check_count[group_key] += 1
                        if subtitle_check_count[group_key] % 2 == 0:
                            logging.info(f"[{group_key}] 等待字幕... ({subtitle_check_count[group_key]})")
            
            time.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            logging.info("程序退出")
            break
        except Exception as e:
            logging.error(f"主循环异常: {e}")
            logging.error(traceback.format_exc())
            time.sleep(5)

if __name__ == "__main__":
    main_loop()