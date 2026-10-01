import shutil
import logging
import sys
import re
import os
import time
from pathlib import Path

# 路径设置：引用 shared 配置
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))
from config import OUTPUT_DIR, OUTPUT_EXTENSION, INCOMING_DIR, PROCESSED_DIR

MERGED_DIR = OUTPUT_DIR

def delete_path(path: Path):
    """安全删除文件或目录"""
    if not path.exists():
        return
    try:
        if path.is_dir():
            shutil.rmtree(path)
            logging.info(f"🗑️ [清理] 已删除碎片目录: {path.name}")
        else:
            path.unlink()
            logging.info(f"🗑️ [清理] 已删除文件: {path.name}")
    except Exception as e:
        logging.error(f"❌ 删除失败 {path}: {e}")

def is_folder_actively_writing(folder: Path, threshold_seconds: int = 300) -> bool:
    """检查文件夹在指定秒数内是否仍有写入活跃，防止录制中途误删"""
    try:
        ts_files = list(folder.glob("*.ts"))
        if not ts_files:
            return False
        latest_mtime = max(f.stat().st_mtime for f in ts_files)
        return (time.time() - latest_mtime) < threshold_seconds
    except Exception:
        return False

def extract_search_pattern(filename_stem: str):
    """
    【中文备注 - 安全修复】
    从文件名中提取核心搜索关键字（支持分卷后缀如 (Part 1)）。
    若正则匹配失败返回 None，严禁返回通配符 '*'，
    防止引发对根目录下所有子目录的误匹配与误删。
    """
    match = re.search(r' - (.*?) \d{6}(?:\s*\(Part\s*\d+\))?$', filename_stem)
    if match:
        member_signature = match.group(1).strip()
        return f"*{member_signature}*"
    logging.warning(f"⚠️ 无法提取成员签名: {filename_stem}，安全中止以防误删")
    return None

def find_and_delete_incoming_fragments(target_mp4_name: str, search_root: Path):
    """
    针对 incoming_ts 的安全清理逻辑：
    1. 必须在 MERGED_DIR 发现成品标记 (.uploaded 或 .merged)
    2. 碎片目录内必须存在 filelist.txt (证明合并流程已启动并读取过该目录)
    3. 碎片目录在过去 5 分钟内不得有新写入（防止分段合并后正在录制下一段时被误删）
    """
    stem = target_mp4_name.replace(OUTPUT_EXTENSION, "")
    pattern = extract_search_pattern(stem)
    # 【中文备注】如果无法精确提取成员标识，直接跳过本次清理，保证安全
    if not pattern:
        logging.warning(f"⏭️ [安全跳过] 无法获取成员匹配模式，跳过原始切片清理: {target_mp4_name}")
        return

    candidates = search_root.glob(pattern)

    # 检查全局完成标记
    upload_marker = MERGED_DIR / f"{target_mp4_name}.uploaded"
    merged_marker_global = MERGED_DIR / f"{stem}.merged"

    if not (upload_marker.exists() or merged_marker_global.exists()):
        logging.info(f"⏭️ [跳过] {target_mp4_name} 尚未完成合并或上传，暂不清理原始 TS")
        return

    for folder in candidates:
        if not folder.is_dir(): continue
        
        # 保护锁：如果文件夹最近还在写切片，严禁删除
        if is_folder_actively_writing(folder, 300):
            logging.info(f"⏭️ [保护跳过] 文件夹 {folder.name} 最近仍有写入活跃，可能处于录制中，跳过清理")
            continue

        # 你的核心判断：是否存在 filelist.txt
        if (folder / "filelist.txt").exists():
            logging.info(f"✅ [确认] 发现 filelist.txt 且成品已就绪，删除原始碎片: {folder.name}")
            delete_path(folder)

def find_and_delete_processed_fragments(target_mp4_name: str, search_root: Path):
    """针对 processed_ts 的逻辑：依然依赖 .merged 标记进行精准确认"""
    stem = target_mp4_name.replace(OUTPUT_EXTENSION, "")
    pattern = extract_search_pattern(stem)
    # 【中文备注】如果无法精确提取成员标识，直接跳过本次清理，保证安全
    if not pattern:
        logging.warning(f"⏭️ [安全跳过] 无法获取成员匹配模式，跳过拉伸切片清理: {target_mp4_name}")
        return

    candidates = search_root.glob(pattern)

    for folder in candidates:
        if not folder.is_dir(): continue
        merged_marker = folder / ".merged"
        if merged_marker.exists():
            content = merged_marker.read_text(encoding='utf-8', errors='ignore')
            if f"Output File: {target_mp4_name}" in content:
                delete_path(folder)

def cleanup_video_resources(video_filename_stem: str):
    """执行深度清理"""
    target_mp4_name = f"{video_filename_stem}{OUTPUT_EXTENSION}"
    logging.info(f"🧹 开始深度清理任务: {target_mp4_name}")

    # 1. 清理 Incoming (使用 filelist.txt 判断)
    find_and_delete_incoming_fragments(target_mp4_name, INCOMING_DIR)

    # 2. 清理 Processed (使用 .merged 标记判断)
    find_and_delete_processed_fragments(target_mp4_name, PROCESSED_DIR)

    # 3. 清理成品及日志
    delete_path(MERGED_DIR / target_mp4_name)
    delete_path(MERGED_DIR / f"{target_mp4_name}.uploaded")
    delete_path(MERGED_DIR / f"{video_filename_stem}.merged") 
    delete_path(MERGED_DIR / f"{video_filename_stem}_log.txt") 

    logging.info(f"✨ 清理任务结束: {video_filename_stem}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        cleanup_video_resources(sys.argv[1])
    else:
        print("用法: python cleanup.py [视频文件名(不带后缀)]")