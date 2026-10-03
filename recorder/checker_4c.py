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
from upscaler import get_frame_rate, upscale_file  # 需确保 recorder/upscaler.py 存在
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
        # 1. 优先尝试从已启用的成员配置中精准匹配 (处理官方号、特别频道等多词ID)
        for m in sorted(ENABLED_MEMBERS, key=lambda x: len(x.get('name_en', '')), reverse=True):
            m_id = m.get('id', '')
            m_en = m.get('name_en', '')
            m_jp = m.get('name_jp', '')
            
            if m_en and m_en.lower() in folder_name.lower():
                return m_id
            if m_jp and m_jp in folder_name:
                return m_id
            if m_id and m_id.lower() in folder_name.lower():
                return m_id

        # 2. 如果配置中未命中，回退到按单词切分的启发式规则
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

def get_ss_num_from_path(f):
    m = re.search(r'ss-(\d+)', f.name)
    return int(m.group(1)) if m else -1

def get_chunk_range(mp4_path: Path):
    """从 chunk_000000_000499.mp4 提取起止序号"""
    m = re.search(r'chunk_(\d+)_(\d+)\.mp4', mp4_path.name)
    if m:
        return int(m.group(1)), int(m.group(2))
    return -1, -1

def parse_filelist_txt(filelist_path: Path):
    """从 filelist_partX.txt 或 filelist.txt 读取所有 .ts 文件路径并统一映射为 4C 本地路径"""
    ts_files = []
    if not filelist_path.exists():
        return ts_files
    folder = filelist_path.parent
    try:
        with open(filelist_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("file '") and line.endswith("'"):
                    fname = Path(line[6:-1]).name
                    ts_files.append(folder / fname)
    except Exception as e:
        logging.error(f"读取清单失败 {filelist_path}: {e}")
    return ts_files

def process_live_folder_upscale(incoming_folder: Path, processed_folder: Path, is_last: bool = False, src_files: list = None):
    """
    核心任务：将 Incoming (360p) 的文件拉伸到 Processed (1080p)
    """
    if not incoming_folder.exists():
        return

    processed_folder.mkdir(parents=True, exist_ok=True)
    if src_files is None:
        src_files = sorted(list(incoming_folder.glob("*.ts")),
                           key=lambda f: [int(c) if c.isdigit() else c.lower()
                                          for c in re.split(r'(\d+)', f.name)])

    if not src_files:
        return

    fps = get_frame_rate(src_files)

    # 按序号断层切分成连续段
    def get_ss_num(f):
        m = re.search(r'ss-(\d+)', f.name)
        return int(m.group(1)) if m else -1

    # 切分连续段
    segments = []
    current_seg = [src_files[0]]
    for i in range(1, len(src_files)):
        prev_num = get_ss_num(src_files[i-1])
        curr_num = get_ss_num(src_files[i])
        if curr_num - prev_num == 1:
            current_seg.append(src_files[i])
        else:
            segments.append(current_seg)
            current_seg = [src_files[i]]
    segments.append(current_seg)

    # 每段最多500个，超过500再细分
    chunks = []
    for seg in segments:
        for i in range(0, len(seg), 500):
            chunks.append(seg[i:i+500])

    for chunk in chunks:
        chunk_last_num = get_ss_num(chunk[-1])
        is_chunk_sealed = is_last
        if not is_chunk_sealed:
            for pf in incoming_folder.glob("filelist_part*.txt"):
                part_ts = parse_filelist_txt(pf)
                if part_ts and get_ss_num_from_path(part_ts[-1]) >= chunk_last_num:
                    is_chunk_sealed = True
                    break

        # 不足500个且不是最后阶段或未封包，跳过
        if len(chunk) < 500 and not is_chunk_sealed:
            continue

        first_num = get_ss_num(chunk[0])
        last_num = get_ss_num(chunk[-1])
        out_name = f"chunk_{first_num:06d}_{last_num:06d}.mp4"
        dst = processed_folder / out_name

        if dst.exists() and dst.stat().st_size > 0:
            continue

        tmp_list = processed_folder / f".tmp_{first_num}.txt"
        with open(tmp_list, "w", encoding="utf-8") as f:
            for ts in chunk:
                f.write(f"file '{ts.resolve()}'\n")

        logging.info(f"⚡ [{incoming_folder.name}] 拉伸分组 {out_name} ({len(chunk)}个分片)")
        upscale_file(tmp_list, dst, fps=fps, is_filelist=True)
        tmp_list.unlink(missing_ok=True)

def get_ss_num_from_path(f):
    m = re.search(r'ss-(\d+)', f.name)
    return int(m.group(1)) if m else -1

def check_group_ready_to_merge(group_folders):
    for folder in group_folders:
        signal_file = folder / FILELIST_NAME
        if not signal_file.exists():
            return False, f"等待 3C 同步信号: {folder.name}"

        proc_folder = PROCESSED_DIR / folder.name
        if not proc_folder.exists():
            return False, f"等待创建拉伸目录: {proc_folder.name}"

        src_files = sorted(list(folder.glob("*.ts")),
                           key=lambda f: [int(c) if c.isdigit() else c.lower()
                                          for c in re.split(r'(\d+)', f.name)])
        if not src_files:
            continue

        last_ts = src_files[-1]
        processed_mp4s = list(proc_folder.glob("chunk_*.mp4"))
        if not processed_mp4s:
            return False, f"拉伸进行中: 0个chunk完成"

        last_ts_num = get_ss_num_from_path(last_ts)
        last_chunk_done = any(f"{last_ts_num:06d}" in mp4.name for mp4 in processed_mp4s)

        if not last_chunk_done:
            return False, f"拉伸进行中: 最后chunk未完成"

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
        ts_files = sorted(list(processed_dir.glob("chunk_*.mp4")), key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f.name)])
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

            if isinstance(task, dict):
                # 自定义分卷合并任务
                item = task
                task_name = item.get('name', 'unknown')
                try:
                    logging.info(f"🔄 [合并队列] 启动自定义/分卷合并: {task_name}")
                    merge_once(custom_item=item)
                    logging.info(f"✅ [合并队列] 完成: {task_name}")
                except Exception as e:
                    logging.error(f"❌ [合并队列] 失败 {task_name}: {e}")
                    logging.error(traceback.format_exc())
                finally:
                    merge_queue.task_done()
                continue
            
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
                    part_files = sorted(list(folder.glob("filelist_part*.txt")),
                                       key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f.name)])
                    if part_files:
                        all_part_ts = set()
                        for pf in part_files:
                            pts = parse_filelist_txt(pf)
                            if pts:
                                all_part_ts.update(pts)
                                process_live_folder_upscale(folder, proc_folder, is_last=True, src_files=pts)
                        
                        all_ts = sorted(list(folder.glob("*.ts")),
                                        key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f.name)])
                        remaining_ts = [f for f in all_ts if f not in all_part_ts]
                        if remaining_ts:
                            is_last = (folder / FILELIST_NAME).exists()
                            process_live_folder_upscale(folder, proc_folder, is_last=is_last, src_files=remaining_ts)
                    else:
                        is_last = (folder / FILELIST_NAME).exists()
                        process_live_folder_upscale(folder, proc_folder, is_last=is_last)

                # === 步骤 A.2: 检查中途分卷 (如 Part 1, Part 2...) 是否已封包并完成拉伸 ===
                for folder in group_folders:
                    proc_folder = PROCESSED_DIR / folder.name
                    part_files = sorted(list(folder.glob("filelist_part*.txt")),
                                       key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f.name)])
                    for pf in part_files:
                        m = re.search(r'filelist_part(\d+)\.txt', pf.name)
                        if not m:
                            continue
                        part_num = int(m.group(1))
                        part_key = f"{folder.name}_part{part_num}"
                        
                        if part_key in submitted_merges or (proc_folder / f".merged_part{part_num}").exists():
                            continue
                        
                        # 若总直播已结束 (filelist.txt 存在) 且此分卷是最后一个分卷，交由后续步骤 B 处理收尾
                        is_total_finished = (folder / FILELIST_NAME).exists()
                        is_latest_part = (part_num == len(part_files))
                        if is_total_finished and is_latest_part:
                            continue
                        
                        # 检查此分卷的 TS 切片拉伸是否全部就绪
                        part_ts_list = parse_filelist_txt(pf)
                        if not part_ts_list:
                            continue
                        
                        # 4C 端防漏传自检：确保该分卷所有切片已在 4C 本地落盘
                        missing_on_4c = [p for p in part_ts_list if not (folder / p.name).exists()]
                        if missing_on_4c:
                            logging.warning(f"⚠️ [4C分卷] {folder.name} Part {part_num} 发现 {len(missing_on_4c)} 个切片尚未同步到 4C (例如 {missing_on_4c[0].name})，等待 3C 补传...")
                            continue
                        
                        part_ts_set = set(get_ss_num_from_path(p) for p in part_ts_list)
                        last_ss_num = get_ss_num_from_path(part_ts_list[-1])
                        
                        processed_mp4s = sorted(list(proc_folder.glob("chunk_*.mp4")),
                                                key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f.name)])
                        
                        part_chunks = []
                        for mp4 in processed_mp4s:
                            c_start, c_end = get_chunk_range(mp4)
                            if c_start in part_ts_set and c_end in part_ts_set:
                                part_chunks.append(mp4)
                        
                        last_chunk_done = any(get_chunk_range(mp4)[1] == last_ss_num for mp4 in part_chunks)
                        if not last_chunk_done or not part_chunks:
                            continue
                        
                        proc_part_filelist = proc_folder / f"filelist_part{part_num}.txt"
                        with open(proc_part_filelist, "w", encoding="utf-8") as f:
                            for c in part_chunks:
                                f.write(f"file '{c.resolve()}'\n")
                        
                        part_name = f"{folder.name} (Part {part_num})"
                        logging.info(f"📋 [4C分卷] 中途分卷 Part {part_num} 拉伸完成，提交合并: {part_name}")
                        custom_item = {
                            'type': 'single',
                            'filelist': proc_part_filelist,
                            'name': part_name,
                            'folders': [proc_folder, folder],
                            'is_partial': True
                        }
                        merge_queue.put(custom_item)
                        submitted_merges.add(part_key)

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
                        
                        # 检查是否有分卷历史
                        first_incoming = group_folders[0]
                        existing_part_files = sorted(list(first_incoming.glob("filelist_part*.txt")),
                                                    key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f.name)])
                        
                        if existing_part_files:
                            # 曾经分卷过，这是收尾分卷
                            final_part_num = len(existing_part_files)
                            proc_folder = PROCESSED_DIR / first_incoming.name
                            
                            # 防漏传自检：确保收尾分卷所有切片已在 4C 本地落盘
                            final_ts_files = parse_filelist_txt(existing_part_files[-1])
                            missing_final = [p for p in final_ts_files if not (first_incoming / p.name).exists()]
                            if missing_final:
                                logging.warning(f"⚠️ [4C收尾] {first_incoming.name} 发现 {len(missing_final)} 个切片尚未同步到 4C (例如 {missing_final[0].name})，等待 3C 补传...")
                                continue
                            
                            # 提取所有前序分卷的切片序号集合
                            all_prev_ts_nums = set()
                            if final_part_num > 1:
                                for prev_pf in existing_part_files[:-1]:
                                    for p in parse_filelist_txt(prev_pf):
                                        all_prev_ts_nums.add(get_ss_num_from_path(p))
                            
                            # 收集属于收尾分卷的所有 chunk (其起始切片不属于任何前序分卷)
                            processed_mp4s = sorted(list(proc_folder.glob("chunk_*.mp4")),
                                                    key=lambda f: [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', f.name)])
                            final_chunks = [mp4 for mp4 in processed_mp4s if get_chunk_range(mp4)[0] not in all_prev_ts_nums]
                            
                            if not final_chunks:
                                # 无新切片，直接打 .merged 标记结束
                                (proc_folder / ".merged").touch(exist_ok=True)
                                (first_incoming / ".merged").touch(exist_ok=True)
                                logging.info(f"[{group_key}] 收尾分卷无新切片，直接标记完成")
                                submitted_merges.add(group_key)
                            else:
                                proc_final_filelist = proc_folder / f"filelist_part{final_part_num}.txt"
                                with open(proc_final_filelist, "w", encoding="utf-8") as f:
                                    for c in final_chunks:
                                        f.write(f"file '{c.resolve()}'\n")
                                
                                part_name = f"{first_incoming.name} (Part {final_part_num})"
                                custom_item = {
                                    'type': 'single',
                                    'filelist': proc_final_filelist,
                                    'name': part_name,
                                    'folders': [proc_folder, first_incoming],
                                    'is_partial': False  # 收尾分卷打 .merged 标记
                                }
                                merge_queue.put(custom_item)
                                submitted_merges.add(group_key)
                        else:
                            # 正常未分卷直播，先进行防漏传核验
                            all_missing = []
                            for g_folder in group_folders:
                                sig_file = g_folder / FILELIST_NAME
                                if sig_file.exists():
                                    folder_ts = parse_filelist_txt(sig_file)
                                    missing_in_folder = [p for p in folder_ts if not (g_folder / p.name).exists()]
                                    all_missing.extend(missing_in_folder)
                            if all_missing:
                                logging.warning(f"⚠️ [4C收尾] {group_key} 发现 {len(all_missing)} 个切片尚未同步到 4C (例如 {all_missing[0].name})，等待 3C 补传...")
                                continue

                            # 100% 保持原有流程
                            finalize_upscale_group(group_folders)
                            processed_group_folders = [PROCESSED_DIR / f.name for f in group_folders]
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