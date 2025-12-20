import time
import subprocess
import fcntl
import os
import re
from collections import defaultdict
from pathlib import Path
from config import *

try:
    from upload_youtube import upload_all_pending_videos
    UPLOAD_AVAILABLE = True
except ImportError:
    UPLOAD_AVAILABLE = False
    log("上传模块不可用，跳过自动上传功能")

class FileLock:
    """文件锁类，防止多个进程同时处理同一个文件"""
    
    def __init__(self, lock_file_path: Path, timeout: int = 300):
        self.lock_file_path = lock_file_path
        self.timeout = timeout
        self.lock_file = None
        
    def __enter__(self):
        """获取锁"""
        # 确保锁目录存在
        self.lock_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            self.lock_file = open(self.lock_file_path, 'w')
            # 尝试获取排他锁
            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # 写入进程信息
            self.lock_file.write(f"PID: {os.getpid()}\nTime: {time.time()}\n")
            self.lock_file.flush()
            return self
        except (OSError, IOError):
            if self.lock_file:
                self.lock_file.close()
            return None
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """释放锁"""
        if self.lock_file:
            try:
                fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_UN)
                self.lock_file.close()
                # 删除锁文件
                if self.lock_file_path.exists():
                    self.lock_file_path.unlink()
            except:
                pass

def extract_folder_key(folder_name: str) -> str:
    """提取文件夹名称的关键部分用于分组,去掉日期和末尾时间戳"""
    # 先去掉末尾的6位数字时间戳
    pattern1 = r'\s+\d{6}$'
    name = re.sub(pattern1, '', folder_name)
    
    # 再去掉开头的日期部分 "YYMMDD Showroom - "
    pattern2 = r'^\d{6}\s+Showroom\s+-\s+'
    key = re.sub(pattern2, '', name)
    
    return key

def find_ready_folders(parent_dir: Path):
    """查找所有准备好合并的文件夹，按名称排序合并"""
    folders = [f for f in parent_dir.iterdir() if f.is_dir()]
    
    # 找出所有有filelist.txt但没有.mp4的文件夹
    candidate_folders = []
    for folder in folders:
        # 检查是否有合并标记文件
        merged_marker = folder / ".merged"
        if merged_marker.exists():
            continue
            
        filelist_txt = folder / FILELIST_NAME
        output_file = OUTPUT_DIR / f"{folder.name}{OUTPUT_EXTENSION}"
        
        # 1. 检查是否存在 filelist.txt 且没有输出文件
        if filelist_txt.exists() and not output_file.exists():
            
            # 2. 【核心修正】新增检查：确保 filelist.txt 包含有效文件行
            has_valid_files = False
            try:
                with open(filelist_txt, 'r', encoding='utf-8') as f:
                    for line in f:
                        # 只有当行内容是以 "file '" 开头时才认为它是有效文件
                        if line.strip().startswith("file '"):
                            has_valid_files = True
                            break
            except Exception:
                # 文件读取失败也跳过
                continue 

            if has_valid_files:
                # 只有包含有效文件的文件夹才会被加入到候选列表
                candidate_folders.append(folder)
    
    if not candidate_folders:
        return []
    
# 按文件夹名称的关键部分分组
    groups = defaultdict(list)
    for folder in candidate_folders:
        key = extract_folder_key(folder.name)
        groups[key].append(folder)
    
    # 为每个组创建合并项目
    merge_items = []
    for key, folder_list in groups.items():
        folder_list.sort(key=lambda x: x.name)
        
        if len(folder_list) == 1:
            folder = folder_list[0]
            merge_items.append({
                'type': 'single',
                'filelist': folder / FILELIST_NAME,
                'name': folder.name,
                'folders': [folder]
            })
        else:
            merged_name = folder_list[0].name
            merged_filelist = create_combined_filelist(folder_list, merged_name)
            merge_items.append({
                'type': 'merged',
                'filelist': merged_filelist,
                'name': merged_name,
                'folders': folder_list
            })
    
    return merge_items

def create_combined_filelist(folders_list, merged_name):
    """创建合并的filelist.txt"""
    temp_dir = Path(OUTPUT_DIR) / ".temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    merged_file = temp_dir / f"{merged_name}_combined.txt"
    
    # 合并所有文件夹的 filelist.txt
    with open(merged_file, 'w') as out:
        for folder in folders_list:
            filelist_path = folder / FILELIST_NAME
            if filelist_path.exists():
                # 注意：使用 'utf-8' 编码读取
                with open(filelist_path, 'r', encoding='utf-8') as f:
                    # 最小化修改：仅写入 FFmpeg 识别的有效行，忽略注释和空行
                    for line in f:
                        stripped_line = line.strip()
                        # 只有当行内容是以 "file '" 开头时才写入
                        if stripped_line.startswith("file '"):
                            out.write(line)
    
    return merged_file

def merge_item(item: dict) -> bool:
    """合并单个项目（可能是单个文件夹或合并的文件夹）"""
    name = item['name']
    filelist_txt = item['filelist']
    output_file = OUTPUT_DIR / f"{name}{OUTPUT_EXTENSION}"
    
    # 创建锁文件路径
    lock_file = LOCK_DIR / f"{name}.merge.lock"
    
    if not filelist_txt.exists():
        log(f"{name} 没有 {FILELIST_NAME}，跳过合并")
        return False

    if output_file.exists():
        log(f"跳过已合并：{name}")
        return True

    # 使用文件锁防止重复合并
    with FileLock(lock_file, MERGE_LOCK_TIMEOUT) as lock:
        if lock is None:
            log(f"{name} 正在被其他进程合并，跳过")
            return False
        
        # 再次检查文件是否存在（双重检查）
        if output_file.exists():
            log(f"跳过已合并：{name}")
            return True
        
        # 确保输出目录存在
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        log(f"开始合并 {name} -> {output_file}")
        
        # 构建 FFmpeg 命令
        ffmpeg_cmd = ["ffmpeg"]
        
        if FFMPEG_HIDE_BANNER:
            ffmpeg_cmd.extend(["-hide_banner"])
        
        ffmpeg_cmd.extend([
            "-loglevel", FFMPEG_LOGLEVEL,
            "-f", "concat", "-safe", "0", "-i", str(filelist_txt),
            "-c", "copy", str(output_file)
        ])
        
        result = subprocess.run(ffmpeg_cmd)

        if result.returncode == 0:
            log(f"{name} 合并完成")
            # --- 修改部分：统一为所有相关的原始文件夹添加标记 ---
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            for folder in item['folders']:  # 无论 single 还是 merged，folders 列表里都有目标文件夹
                marker_file = folder / ".merged"
                try:
                    marker_content = (
                        f"Status: Success\n"
                        f"Merged Time: {timestamp}\n"
                        f"Output File: {name}{OUTPUT_EXTENSION}\n"
                    )
                    marker_file.write_text(marker_content, encoding='utf-8')
                    log(f"已为文件夹 {folder.name} 添加合并标记")
                except Exception as e:
                    log(f"无法为 {folder.name} 创建标记文件: {e}")
            return True
        else:
            log(f"{name} 合并失败，请检查 ffmpeg 日志")
            return False

def merge_all_ready():
    """合并所有准备好的文件夹"""
    ready_items = find_ready_folders(PARENT_DIR)
    
    if not ready_items:
        log("没有找到待合并的文件夹")
        return 0
    
    log(f"找到 {len(ready_items)} 个待合并的文件夹")
    
    success_count = 0
    for folder in ready_items:
        if merge_item(folder):
            success_count += 1

    log(f"成功合并 {success_count} 个视频")
    return success_count

def upload_if_needed(success_count):
    """如果合并成功，通过外部进程异步启动上传任务"""
    
    if ENABLE_AUTO_UPLOAD and success_count > 0 and UPLOAD_AVAILABLE:
        log("🎬 [异步触发] 正在启动独立上传进程...")
        
        try:
            # 1. 配置路径
            VENV_ACTIVATE_DIR = "/home/ubuntu/venv"
            script_path = Path("/home/ubuntu/live-merge-up") / "upload_youtube.py"
            
            # 2. 构造命令 (必须带 -u 确保无缓冲)
            full_command = f"source {VENV_ACTIVATE_DIR}/bin/activate && python3 -u {str(script_path)}"
            
            command = [
                "/bin/bash",
                "-c",
                full_command
            ]
            
            # 3. 异步启动
            # 注意：不设置 stdout 和 stderr，它们会自动继承 merger.py 的输出流
            # 也就是自动写入到 /home/ubuntu/logs/live-merge-up.log
            subprocess.Popen(
                command,
                start_new_session=True 
            )
            
            log("✅ 上传指令已发出，日志将自动追加到当前服务日志文件中。")
            
        except Exception as e:
            log(f"🚨 [启动上传失败]: {e}")

def merge_once(target_folders=None):  # 改成复数
    """执行一次合并操作
    
    Args:
        target_folders: 指定要合并的文件夹列表(属于同一个直播)
    """
    
    if target_folders:
        # 只处理指定的文件夹组
        folders_to_merge = target_folders
        
        # 为这组文件夹创建合并项目
        if len(folders_to_merge) == 1:
            folder = folders_to_merge[0]
            item = {
                'type': 'single',
                'filelist': folder / FILELIST_NAME,
                'name': folder.name,
                'folders': [folder]
            }
        else:
            # 多个文件夹，创建合并filelist
            merged_name = folders_to_merge[0].name
            merged_filelist = create_combined_filelist(folders_to_merge, merged_name)
            item = {
                'type': 'merged',
                'filelist': merged_filelist,
                'name': merged_name,
                'folders': folders_to_merge
            }
        
        # 执行合并
        success = merge_item(item)
        upload_if_needed(1 if success else 0)
        
    else:
        # 原来的逻辑:合并所有准备好的文件夹
        success_count = merge_all_ready()
        upload_if_needed(success_count)

if __name__ == "__main__":
    # 可以选择运行模式
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        merge_once()
    else:
        merge_once()