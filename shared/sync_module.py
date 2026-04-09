import os
import subprocess
import logging
import shlex
from pathlib import Path
# 引入配置
from config import (
    REMOTE_IP, REMOTE_PORT, REMOTE_VIDEO_DIR, 
    SYNC_MODE, MAIN_MEMBER_ID, 
    SUBTITLES_SOURCE_ROOT
)

# ==================== 【全局缓存】 ====================
# 改成按目录(直播场次)缓存
_stream_height_cache = {}  # 格式: {"hashimoto_haruna_20250206": 720}

def get_video_height_for_stream(stream_dir):
    """
    【工具】获取直播场次的分辨率
    策略: 尝试检测目录下前5个 .ts 文件,任意1个成功就返回
    """
    stream_dir = Path(stream_dir)
    cache_key = str(stream_dir)
    
    # 1. 查缓存
    if cache_key in _stream_height_cache:
        cached = _stream_height_cache[cache_key]
        if isinstance(cached, int):
            return cached
        elif cached == 'FAILED':
            return None
    
    # 2. 获取目录下所有 .ts 文件
    ts_files = sorted(stream_dir.glob("*.ts"))
    
    if not ts_files:
        _stream_height_cache[cache_key] = 'FAILED'
        return None
    
    # 3. 尝试检测前5个文件
    max_attempts = min(5, len(ts_files))
    
    for i in range(max_attempts):
        test_file = ts_files[i]
        
        cmd = [
            "ffprobe", 
            "-v", "error", 
            "-select_streams", "v:0", 
            "-show_entries", "stream=height", 
            "-of", "csv=p=0", 
            str(test_file.resolve())
        ]
        
        try:
            result = subprocess.run(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                text=True, 
                timeout=5
            )
            
            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    # === 【关键修复:只取第一行】 ===
                    first_line = output.split('\n')[0].strip()
                    if first_line.isdigit():
                        height = int(first_line)
                        _stream_height_cache[cache_key] = height
                        logging.info(f"✅ 分辨率: {stream_dir.name} = {height}p")
                        return height
                    # ================================
            
        except subprocess.TimeoutExpired:
            logging.debug(f"⏰ ffprobe 超时: {test_file.name}")
            continue
        except Exception as e:
            logging.debug(f"❌ ffprobe 异常: {test_file.name} - {e}")
            continue
    
    # 4. 所有文件都失败
    _stream_height_cache[cache_key] = 'FAILED'
    logging.warning(f"⚠️ 分辨率检测失败(已尝试{max_attempts}个文件): {stream_dir.name}")
    return None


def get_video_height(file_path):
    """
    【工具】获取视频高度 (入口函数)
    - .ts 文件: 按目录检测(调用 get_video_height_for_stream)
    - 其他文件: 按文件检测
    """
    path_obj = Path(file_path)
    
    if path_obj.suffix == '.ts':
        # .ts 文件交给场次检测逻辑
        return get_video_height_for_stream(path_obj.parent)
    else:
        # 其他文件(如 .mp4)仍按原逻辑单独检测
        cache_key = str(path_obj)
        
        if cache_key in _stream_height_cache:
            cached = _stream_height_cache[cache_key]
            return cached if isinstance(cached, int) else None
        
        # 单文件检测逻辑(保持原样)
        cmd = [
            "ffprobe", "-v", "error", 
            "-select_streams", "v:0", 
            "-show_entries", "stream=height", 
            "-of", "csv=p=0", 
            str(path_obj)
        ]
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=2)
            output = result.stdout.strip()
            if output and output.isdigit():
                height = int(output)
                _stream_height_cache[cache_key] = height
                return height
        except Exception:
            pass
        
        _stream_height_cache[cache_key] = 'FAILED'
        return None

# ==================== 【裁判逻辑】 ====================
def should_run_local_upload(file_path) -> bool:
    """
    判断是否应该在本地(3C)上传。
    返回 True:  3C 必须上传 (高清，或者 4C 不管的人)
    返回 False: 3C 跳过 (因为 4C 会负责处理)
    """
    path_obj = Path(file_path)
    
    # 1. 模式 OFF: 4C 罢工，3C 必须兜底全干
    if SYNC_MODE == "off":
        return True

    # 2. 检查分辨率
    height = get_video_height(str(path_obj))
    
    # === 【关键修改：无罪推定】 ===
    # 只有【明确检测到】是 720P+ 才强制 3C 本地传。
    # 如果 height is None (检测失败)，默认认为可能是低清，交给下面的逻辑判断。
    if height is not None and height >= 720:
        return True # 确认为高清 -> 3C 必须自己传

    # --- 以下是低清视频 (或未知分辨率) 的判断 ---

    if path_obj.suffix == '.ts':
        target_name = path_obj.parent.name.lower()
    else:
        target_name = path_obj.name.lower()

    # 3. 模式 ALL: 4C 负责所有人 -> 3C 全部跳过
    if SYNC_MODE == "all":
        return False

    # 4. 模式 MAIN: 4C 仅负责主推
    if SYNC_MODE == "main":
        if not MAIN_MEMBER_ID: 
            return True
        
        clean_id = MAIN_MEMBER_ID.lower().replace('_', '').replace(' ', '')
        clean_target = target_name.replace('_', '').replace(' ', '') # target_name 已经是 lower 了
        
        if clean_id in clean_target:
            return False # 匹配成功 (是主推) -> 3C 放手
        else:
            return True # 匹配失败 (不是主推) -> 3C 自己干

    return True # 默认兜底

# ==================== 【同步逻辑】 ====================
class RemoteSyncer:
    def __init__(self):
        self.synced_set = set() 

    def _mark_synced(self, file_path):
        self.synced_set.add(file_path)
        # 防止集合无限膨胀，超过 15000 条清理一次
        if len(self.synced_set) > 15000:
            self.synced_set.clear()

    def sync_to_4c(self, local_path, member_id=None):
        """同步单个文件到 4C"""
        if not REMOTE_IP or not REMOTE_PORT or SYNC_MODE == "off":
            return

        # 1. 主推过滤 (忽略大小写)
        if SYNC_MODE == "main":
            if not member_id:
                return
            if MAIN_MEMBER_ID and member_id.lower() != MAIN_MEMBER_ID.lower():
                return

        file_path = str(local_path)
        
        # 2. 简单的去重和存在性检查
        if file_path in self.synced_set or not os.path.exists(file_path):
            return

        # 3. 分辨率检查
        if file_path.endswith(".ts"):
            height = get_video_height(file_path)
            
            # 【核心逻辑修正：无罪推定】
            # 只有【明确检测到】是 720P 以上才拦截。
            # 如果 height is None (检测失败)，放行 (视为低清)，确保不漏传。
            if height is not None and height >= 720:
                self.synced_set.add(file_path) 
                return 

        folder_name = Path(local_path).parent.name
        
        # 1. 确保基础目录以斜杠结尾，避免连字符错误
        remote_base = str(REMOTE_VIDEO_DIR).rstrip('/') + '/'
        # 2. 完整的远程目标文件夹路径
        remote_folder_path = f"{remote_base}{folder_name}/"
        
        # --- 3. 准备 Rsync 命令 ---
        ssh_opts = f"ssh -p {REMOTE_PORT} -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o ControlMaster=auto -o ControlPath=/tmp/ssh_mux_%h_%p_%r -o ControlPersist=5m"
        
        # 使用 shlex.quote 对远程 mkdir 的路径进行“强力”转义，应对 shell 二次解析
        safe_remote_mkdir_dir = shlex.quote(remote_folder_path)
        remote_mkdir_cmd = f"mkdir -p {safe_remote_mkdir_dir} && rsync"
        
        cmd = [
            "rsync", "-az", "--ignore-existing", "--partial",
            "--timeout=30",
            "-e", ssh_opts,
            "--rsync-path", remote_mkdir_cmd,
            file_path,  # 本地路径
            f"ubuntu@{REMOTE_IP}:{remote_folder_path}"  # 远程路径
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=40)
            if result.returncode == 0:
                self._mark_synced(file_path)
            else:
                logging.warning(f"⚠️ [Sync] Fail: {result.stderr.strip()}")
        except subprocess.TimeoutExpired:
            logging.error(f"⏰ [Sync] Timeout: {file_path}")
        except Exception as e:
            logging.error(f"❌ [Sync] Exception: {str(e)}")

    def sync_filelist_and_audit(self, filelist_path, member_id=None):
        """
        同步 filelist.txt，并在同步前【审计】内容。
        """
        # ==================== 【关键修复：进门先查身份证】 ====================
        # 如果是 main 模式，且当前成员不是主推，直接忽略！
        # 解决：非主推成员的日志误报问题
        if SYNC_MODE == "main":
            if not member_id:
                return
            if MAIN_MEMBER_ID and member_id.lower() != MAIN_MEMBER_ID.lower():
                return
        # =================================================================

        path_obj = Path(filelist_path)
        if not path_obj.exists():
            return

        # 1. --- 审计阶段 (Audit Phase) ---
        try:
            with open(path_obj, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            for line in lines:
                line = line.strip()
                if line.startswith('file ') and "'" in line:
                    parts = line.split("'")
                    if len(parts) >= 2:
                        ts_path_str = parts[1]
                        ts_path_obj = Path(ts_path_str)
                        
                        if not ts_path_obj.is_absolute():
                            ts_path_obj = path_obj.parent / ts_path_str
                            ts_path_str = str(ts_path_obj)

                        # 【补漏逻辑】
                        if ts_path_str.endswith('.ts') and \
                           ts_path_str not in self.synced_set and \
                           ts_path_obj.exists():
                            
                            logging.info(f"🕵️ [Audit] 补传: {ts_path_obj.name}")
                            # 调用自身同步
                            self.sync_to_4c(ts_path_obj, member_id)

        except Exception as e:
            logging.error(f"⚠️ [Audit] 审计异常 {path_obj.name}: {e}")

        # 2. --- 最后上传 filelist 本身 ---
        self.sync_to_4c(filelist_path, member_id)

    def sync_subtitles(self):
        """同步字幕目录"""
        if not REMOTE_IP or not SUBTITLES_SOURCE_ROOT or not SUBTITLES_SOURCE_ROOT.exists():
            return

        remote_target = f"ubuntu@{REMOTE_IP}:{SUBTITLES_SOURCE_ROOT}/"

        cmd = [
            "rsync", "-avz", "--quiet",
            "--include=*/",       
            "--include=*.json",   
            "--include=*.ass",    
            "--exclude=*",        
            "--prune-empty-dirs", 
            "-e", f"ssh -p {REMOTE_PORT} -o StrictHostKeyChecking=no",
            str(SUBTITLES_SOURCE_ROOT) + "/", 
            remote_target
        ]

        try:
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            logging.info("✅ [Sync] 字幕文件同步完成")
        except Exception as e:
            logging.error(f"❌ [Sync] 字幕同步失败: {e}")

syncer = RemoteSyncer()