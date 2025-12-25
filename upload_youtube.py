import pickle
import time
import fcntl
import os
import shutil
import json
import signal
import yaml

from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from github_pages_publisher import publish_to_github_pages
from config import *

# 全局变量
LAST_QUOTA_EXHAUSTED_DATE = None
JST = ZoneInfo("Asia/Tokyo")
PACIFIC = ZoneInfo("America/Los_Angeles")
MAX_RETRIES = 5  # 最大重试次数
UPLOAD_DELAY = 60 # 每次重试等待时间（秒）
CHUNK_TIMEOUT_SECONDS = 30 # 30秒

# 加载成员配置
def load_members_config():
    """从 AKB48_members.yaml 加载成员配置"""
    # 确保使用正确的变量名
    try:
        with open(MEMBERS_YAML_PATH, 'r', encoding='utf-8') as f:
            # 建议加上 Loader 避免警告
            data = yaml.load(f, Loader=yaml.FullLoader) 
            
            # 如果 yaml 文件内容为空，data 会是 None，需要做容错处理
            if not data:
                return []
            return data.get('members', [])
    except Exception as e:
        if DEBUG_MODE:
            log(f"加载 {MEMBERS_YAML_PATH.name} 失败: {e}")
        return []

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

def convert_title_to_japanese(title: str) -> str:
    """
    将标题中的英文名字转换为日文名字
    
    Args:
        title: 原始标题
    
    Returns:
        转换后的标题
    """
    converted_title = title

    # ========== 每次上传前重新加载members.json ==========
    MEMBERS = load_members_config()
    if VERBOSE_LOGGING:
        log(f"已重新加载成员配置，共 {len(MEMBERS)} 个成员")
    # ============================================================
    
    # 遍历所有成员，进行名字转换
    for member in MEMBERS:
        en_name = member.get('name_en', '')
        jp_name = member.get('name_jp', '')
        
        if en_name and jp_name:
            # 将英文名替换为日文名
            converted_title = converted_title.replace(en_name, jp_name)
    
    if DEBUG_MODE and converted_title != title:
        log(f"标题转换: {title} -> {converted_title}")
    
    return converted_title

def get_today_utc_date_str():
    """获取今天的UTC日期字符串"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def get_next_retry_time_japan():
    """获取下次重试时间（太平洋时间0点对应的日本时间）"""
    if not YOUTUBE_ENABLE_QUOTA_MANAGEMENT:
        return "配额管理已禁用"
    
    # 下一个太平洋时间配额重置时间 => 对应的日本时间
    now_pacific = datetime.now(PACIFIC)
    next_reset_pacific = now_pacific.replace(
        hour=YOUTUBE_QUOTA_RESET_HOUR_PACIFIC, 
        minute=0, 
        second=0, 
        microsecond=0
    )
    
    # 如果今天的重置时间已过，则选择明天
    if now_pacific >= next_reset_pacific:
        next_reset_pacific += timedelta(days=1)

    next_reset_in_japan = next_reset_pacific.astimezone(JST)
    return next_reset_in_japan.strftime("%Y-%m-%d %H:%M:%S")

def get_authenticated_service():
    """获取已认证的YouTube服务对象"""
    creds = None
    
    # 加载已保存的凭据
    if YOUTUBE_TOKEN_PATH.exists():
        try:
            with open(YOUTUBE_TOKEN_PATH, "rb") as token_file:
                creds = pickle.load(token_file)
        except Exception as e:
            if DEBUG_MODE:
                log(f"加载token失败: {e}")
            creds = None

    # 检查凭据是否有效
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                if DEBUG_MODE:
                    log(f"刷新token失败: {e}")
                creds = None
        
        # 如果凭据无效，重新认证
        if not creds:
            if not YOUTUBE_CLIENT_SECRET_PATH.exists():
                raise FileNotFoundError(f"客户端密钥文件不存在: {YOUTUBE_CLIENT_SECRET_PATH}")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                str(YOUTUBE_CLIENT_SECRET_PATH), YOUTUBE_SCOPES
            )
            creds = flow.run_local_server(port=0)

        # 保存凭据
        try:
            with open(YOUTUBE_TOKEN_PATH, "wb") as token_file:
                pickle.dump(creds, token_file)
        except Exception as e:
            if DEBUG_MODE:
                log(f"保存token失败: {e}")

    return build("youtube", "v3", credentials=creds)

def get_authenticated_service_alt():
    """获取副账号的已认证YouTube服务对象"""
    creds = None
    
    # 加载已保存的凭据
    if YOUTUBE_TOKEN_PATH_ALT.exists():
        try:
            with open(YOUTUBE_TOKEN_PATH_ALT, "rb") as token_file:
                creds = pickle.load(token_file)
        except Exception as e:
            if DEBUG_MODE:
                log(f"加载副账号token失败: {e}")
            creds = None

    # 检查凭据是否有效
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                if DEBUG_MODE:
                    log(f"刷新副账号token失败: {e}")
                creds = None
        
        # 如果凭据无效,重新认证
        if not creds:
            if not YOUTUBE_CLIENT_SECRET_PATH_ALT.exists():
                raise FileNotFoundError(f"副账号客户端密钥文件不存在: {YOUTUBE_CLIENT_SECRET_PATH_ALT}")
            
            flow = InstalledAppFlow.from_client_secrets_file(
                str(YOUTUBE_CLIENT_SECRET_PATH_ALT), YOUTUBE_SCOPES
            )
            creds = flow.run_local_server(port=0)

        # 保存凭据
        try:
            YOUTUBE_TOKEN_PATH_ALT.parent.mkdir(parents=True, exist_ok=True)
            with open(YOUTUBE_TOKEN_PATH_ALT, "wb") as token_file:
                pickle.dump(creds, token_file)
        except Exception as e:
            if DEBUG_MODE:
                log(f"保存副账号token失败: {e}")

    return build("youtube", "v3", credentials=creds)

def is_uploaded(file_path: Path) -> bool:
    """检查文件是否已上传"""
    uploaded_flag = file_path.with_suffix(file_path.suffix + ".uploaded")
    return uploaded_flag.exists()

def mark_as_uploaded(file_path: Path, video_id: str):
    """标记文件为已上传并保存视频ID"""
    uploaded_flag = file_path.with_suffix(file_path.suffix + ".uploaded")
    
    # 将视频ID写入.uploaded文件
    with open(uploaded_flag, 'w', encoding='utf-8') as f:
        f.write(video_id)

def handle_post_upload_actions(file_path: Path):
    """处理上传完成后的操作"""
    if YOUTUBE_DELETE_AFTER_UPLOAD:
        try:
            file_path.unlink()
            if VERBOSE_LOGGING:
                log(f"已删除本地文件: {file_path.name}")
        except Exception as e:
            log(f"删除文件失败: {e}")
    
    elif YOUTUBE_MOVE_AFTER_UPLOAD:
        try:
            # 确保备份目录存在
            YOUTUBE_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
            
            backup_path = YOUTUBE_BACKUP_DIR / file_path.name
            # 如果备份文件已存在，添加时间戳
            if backup_path.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = YOUTUBE_BACKUP_DIR / f"{file_path.stem}_{timestamp}{file_path.suffix}"
            
            shutil.move(str(file_path), str(backup_path))
            if VERBOSE_LOGGING:
                log(f"已移动文件到备份目录: {backup_path.name}")
        except Exception as e:
            log(f"移动文件失败: {e}")

def send_upload_notification(file_name: str, video_id: str, success: bool = True):
    """发送上传完成通知"""
    if not YOUTUBE_ENABLE_NOTIFICATIONS or not YOUTUBE_NOTIFICATION_WEBHOOK_URL:
        return
    
    try:
        import requests
        
        if success:
            message = f"✅ 视频上传成功\n文件: {file_name}\n视频ID: {video_id}\n链接: https://youtu.be/{video_id}"
        else:
            message = f"❌ 视频上传失败\n文件: {file_name}"
        
        # 这里是通用的webhook格式，您可以根据具体服务调整
        payload = {"content": message}
        
        requests.post(YOUTUBE_NOTIFICATION_WEBHOOK_URL, json=payload, timeout=10)
        if VERBOSE_LOGGING:
            log(f"已发送通知: {file_name}")
    except Exception as e:
        if DEBUG_MODE:
            log(f"发送通知失败: {e}")

def add_video_to_playlist(youtube, video_id: str, playlist_id: str):
    """将视频添加到播放列表"""
    try:
        request = youtube.playlistItems().insert(
            part="snippet",
            body={
                "snippet": {
                    "playlistId": playlist_id,
                    "resourceId": {
                        "kind": "youtube#video",
                        "videoId": video_id
                    }
                }
            }
        )
        response = request.execute()
        if VERBOSE_LOGGING:
            log(f"已添加视频 {video_id} 到播放列表 {playlist_id}")
        return True
    except HttpError as e:
        log(f"添加到播放列表失败: {e}")
        return False

def upload_video(
    file_path: str, 
    title: str = None, 
    description: str = None, 
    tags: list = None, 
    category_id: str = None,
    playlist_id: str = None
) -> str | None:
    """
    上传视频到YouTube
    """
    class UploadTimeout(Exception):
        pass

    def timeout_handler(signum, frame):
        raise UploadTimeout("上传块超时")    

    file_path_obj = Path(file_path)
    if not file_path_obj.exists():
        log(f"文件不存在: {file_path}")
        return None

    # ========== 每次上传前重新加载members.json ==========
    MEMBERS = load_members_config()
    if VERBOSE_LOGGING:
        log(f"已重新加载成员配置，共 {len(MEMBERS)} 个成员")

    # 判断是否是橋本陽菜的视频
    # 检查文件名中是否包含橋本陽菜的英文或日文名
    is_hashimoto = False
    for member in MEMBERS:
        if member.get('id') == 'hashimoto_haruna':
            en_name = member.get('name_en', '')
            jp_name = member.get('name_jp', '')
            
            if en_name in file_path_obj.stem or jp_name in file_path_obj.stem:
                is_hashimoto = True
                break
    
    try:
        if is_hashimoto:
            youtube = get_authenticated_service()
            if VERBOSE_LOGGING:
                log("使用主账号上传 (橋本陽菜)")
        else:
            youtube = get_authenticated_service_alt()
            if VERBOSE_LOGGING:
                log("使用副账号上传 (其他成员)")
    except Exception as e:
        log(f"获取YouTube服务失败: {e}")
        return None
    
    # 检测视频属于哪个成员,并获取其YouTube配置
    member_config = None
    for member in MEMBERS:
        en_name = member.get('name_en', '')
        jp_name = member.get('name_jp', '')

        if (en_name and en_name in file_path_obj.stem) or \
           (jp_name and jp_name in file_path_obj.stem):
            member_config = member.get('youtube', {})
            if VERBOSE_LOGGING:
                log(f"检测到成员: {jp_name or en_name}")
            break

    # 使用配置的默认值和文件名处理标题
    if title is None:
        # 优先使用成员配置的标题模板
        if member_config and member_config.get('title_template'):
            title = member_config['title_template']
        elif YOUTUBE_DEFAULT_TITLE:
            title = YOUTUBE_DEFAULT_TITLE
        else:
            # 使用文件名作为标题
            title = file_path_obj.stem

        # 应用日文名字转换
        title = convert_title_to_japanese(title)

    if description is None:
        upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 优先使用成员配置的描述模板
        if member_config and member_config.get('description_template'):
            description = member_config['description_template'].format(upload_time=upload_time)
        else:
            description = YOUTUBE_DEFAULT_DESCRIPTION.format(upload_time=upload_time)

    if tags is None:
        # 优先使用成员配置的标签
        if member_config and member_config.get('tags'):
            tags = member_config['tags'].copy()
        else:
            tags = YOUTUBE_DEFAULT_TAGS.copy()

    if category_id is None:
        # 优先使用成员配置的分类
        if member_config and member_config.get('category_id'):
            category_id = member_config['category_id']
        else:
            category_id = YOUTUBE_DEFAULT_CATEGORY_ID

    if playlist_id is None:
        # 优先使用成员配置的播放列表
        if member_config and member_config.get('playlist_id'):
            playlist_id = member_config['playlist_id']
            if VERBOSE_LOGGING:
                log(f"使用成员播放列表: {playlist_id}")
        else:
            playlist_id = YOUTUBE_PLAYLIST_ID
    
    # 构建上传请求
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": category_id,
        },
        "status": {
            "privacyStatus": YOUTUBE_PRIVACY_STATUS
        },
        "madeForKids": False   # 直接声明“不是为儿童制作”
    }
    
    try:
        media = MediaFileUpload(file_path, chunksize=128 * 1024 * 1024, resumable=True)
        request = youtube.videos().insert(
            part="snippet,status",
            body=body,
            media_body=media
        )
        # 这个设置确保 next_chunk 在 180 秒内必须返回。
        request.http.timeout = CHUNK_TIMEOUT_SECONDS 
        log(f"已设置 HTTP 请求超时为 {CHUNK_TIMEOUT_SECONDS} 秒")
    except Exception as e:
        log(f"创建上传请求失败: {e}")
        return None

    # 执行上传
    retry_count = 0
    response = None
    log(f"开始上传: {file_path_obj.name}")
    log(f"视频标题: {title}")

    # 使用外部 while 循环来处理重试
    while retry_count < MAX_RETRIES:        
        # ========== 每次重试都重新创建完整的上传会话 ==========
        try:
            # 如果是重试，重新获取 youtube 服务
            if retry_count > 0:
                log("重新获取YouTube服务...")
                if is_hashimoto:
                    youtube = get_authenticated_service()
                else:
                    youtube = get_authenticated_service_alt()

            # 创建新的上传请求
            media = MediaFileUpload(file_path, chunksize=128 * 1024 * 1024, resumable=True)
            request = youtube.videos().insert(
                part="snippet,status",
                body=body,
                media_body=media
            )
            request.http.timeout = CHUNK_TIMEOUT_SECONDS

            if retry_count > 0:
                log(f"已创建新的上传会话 (重试 {retry_count}/{MAX_RETRIES})")
        except Exception as e:
            log(f"创建上传请求失败: {e}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                time.sleep(UPLOAD_DELAY)
            continue

        try:
            # 内部循环执行断点续传
            while response is None:
                # 设置30秒超时
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(CHUNK_TIMEOUT_SECONDS) 

                try:
                    status, response = request.next_chunk()
                    signal.alarm(0)  # 成功后取消闹钟   

                    if status:
                        progress = int(status.progress() * 100)
                        log(f"上传进度: {progress}% (重试 {retry_count}/{MAX_RETRIES})")    

                except UploadTimeout:
                    signal.alarm(0)  # 取消闹钟
                    raise  # 抛给外层处理
                
            # 成功完成上传，跳出重试循环
            break

        except UploadTimeout as e:
            log(f"上传块在 {CHUNK_TIMEOUT_SECONDS} 秒内无响应")
            retry_count += 1
            response = None  # 重置

            if retry_count < MAX_RETRIES:
                log(f"等待 {UPLOAD_DELAY} 秒后重试 ({retry_count}/{MAX_RETRIES})...")
                time.sleep(UPLOAD_DELAY)
            else:
                log(f"达到最大重试次数 ({MAX_RETRIES})，上传失败。")
                break

        except HttpError as e:
            if e.resp.status == 403 and 'quotaExceeded' in str(e):
                global LAST_QUOTA_EXHAUSTED_DATE
                LAST_QUOTA_EXHAUSTED_DATE = get_today_utc_date_str()
                log("上传配额已用尽")
                raise  # 重新抛出配额错误
            else:
                log(f"上传失败: {e}")
                return None

        except Exception as e:
            # 捕获其他未知错误，并进行重试
            log(f"上传过程中出现未知错误 (重试 {retry_count+1}/{MAX_RETRIES}): {e}")
            retry_count += 1
            if retry_count < MAX_RETRIES:
                log(f"等待 {UPLOAD_DELAY} 秒后重试...")
                time.sleep(UPLOAD_DELAY)
            else:
                log(f"达到最大重试次数 ({MAX_RETRIES})，上传失败。")
                break # 跳出重试循环
    if not response:
        log("上传失败：达到最大重试次数或未收到响应")
        return None

    video_id = response.get("id")
    if not video_id:
        log("上传失败：未获取到视频ID")
        return None
    
    log(f"上传完成，视频ID: {video_id}")

    # 添加到播放列表
    if playlist_id:
        add_video_to_playlist(youtube, video_id, playlist_id)

    return video_id

def handle_merged_video(mp4_path: Path) -> bool:
    """
    处理单个合并后的视频文件
    
    Args:
        mp4_path: MP4文件路径
    
    Returns:
        是否成功处理（True=成功，False=配额用尽或失败）
    """

    # ========== 每次处理前重新加载members.json ==========
    MEMBERS = load_members_config()
    if VERBOSE_LOGGING:
        log(f"已重新加载成员配置，共 {len(MEMBERS)} 个成员")
    # ============================================================

    if is_uploaded(mp4_path):
        if VERBOSE_LOGGING:
            log(f"{mp4_path.name} 已上传，跳过")
        return True
    
    video_id = None
    
    try:
        video_id = upload_video(str(mp4_path))
    except HttpError as e:
        if e.resp.status == 403 and 'quotaExceeded' in str(e):
            global LAST_QUOTA_EXHAUSTED_DATE
            LAST_QUOTA_EXHAUSTED_DATE = get_today_utc_date_str()
            log("检测到上传配额用尽，暂停上传，等待配额重置后继续。")
            return False
        else:
            log(f"上传时发生HTTP错误: {e}")
            send_upload_notification(mp4_path.name, "", False)
            return False
    except Exception as e:
        log(f"上传时发生未知错误: {e}")
        send_upload_notification(mp4_path.name, "", False)
        return False

    if video_id:
        # 获取实际使用的标题、描述和标签(用于保存上传信息)
        title = mp4_path.stem
        title = convert_title_to_japanese(title)

        # 检测成员配置
        member_config = None
        for member in MEMBERS:
            en_name = member.get('name_en', '')
            jp_name = member.get('name_jp', '')
            if (en_name and en_name in mp4_path.stem) or \
               (jp_name and jp_name in mp4_path.stem):
                member_config = member.get('youtube', {})
                break
            
        # 生成描述和标签
        upload_time_for_desc = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if member_config and member_config.get('description_template'):
            description = member_config['description_template'].format(upload_time=upload_time_for_desc)
        else:
            description = YOUTUBE_DEFAULT_DESCRIPTION.format(upload_time=upload_time_for_desc)

        if member_config and member_config.get('tags'):
            tags = member_config['tags'].copy()
        else:
            tags = YOUTUBE_DEFAULT_TAGS.copy()

        mark_as_uploaded(mp4_path, video_id)
        log(f"{mp4_path.name} 上传成功并已标记")
        
        # 发送成功通知
        send_upload_notification(mp4_path.name, video_id, True)
        # 保存上传信息（传递实际使用的上传信息）
        upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        save_upload_info(mp4_path, video_id, title, description, tags, upload_time)
        
        # 处理上传后操作
        handle_post_upload_actions(mp4_path)
        
        return True
    else:
        log(f"{mp4_path.name} 上传失败")
        send_upload_notification(mp4_path.name, "", False)
        return False

def upload_all_pending_videos(directory: Path = None):
    """
    上传目录中所有待上传的视频
    
    Args:
        directory: 包含MP4文件的目录（None时使用配置的OUTPUT_DIR）
    """
    if not ENABLE_AUTO_UPLOAD:
        if DEBUG_MODE:
            log("自动上传功能已禁用")
        return
    
    if directory is None:
        directory = OUTPUT_DIR
    
    global LAST_QUOTA_EXHAUSTED_DATE

    # 创建全局上传锁，防止多个进程同时上传
    upload_lock_file = LOCK_DIR / "upload_global.lock"
    
    with FileLock(upload_lock_file, UPLOAD_LOCK_TIMEOUT) as lock:
        if lock is None:
            if VERBOSE_LOGGING:
                log("其他进程正在上传，跳过本次上传")
            return
        
        _upload_all_pending_videos_internal(directory)

def _upload_all_pending_videos_internal(directory: Path):
    """
    内部上传函数:扫描并上传所有待处理视频
    - 一般错误(网络超时、临时故障):跳过当前视频,继续下一个
    - 严重错误(配额耗尽):立即停止所有上传
    """
    global LAST_QUOTA_EXHAUSTED_DATE
    
    if not directory.exists():
        log(f"目录不存在: {directory}")
        return

    log("=" * 60)
    log("开始扫描待上传视频...")
    log("=" * 60)

    while True:
        today_str = get_today_utc_date_str()
        
        # ========== 1. 检查配额状态 ==========
        if YOUTUBE_ENABLE_QUOTA_MANAGEMENT and LAST_QUOTA_EXHAUSTED_DATE == today_str:
            log("⚠️  检测到配额已耗尽,停止上传")
            log(f"📅 下次重试时间: {get_next_retry_time_japan()}")
            return

        # ========== 2. 扫描待上传文件 ==========
        mp4_files = sorted(directory.glob("*.mp4"))
        pending_files = [f for f in mp4_files if not is_uploaded(f)]

        if not pending_files:
            log("✅ 扫描完成:没有待上传的视频")
            break

        log(f"📦 找到 {len(pending_files)} 个待上传视频")
        log("-" * 60)

        # ========== 3. 逐个处理视频 ==========
        for idx, mp4_file in enumerate(pending_files, 1):
            log(f"[{idx}/{len(pending_files)}] 正在处理: {mp4_file.name}")
            
            try:
                success = handle_merged_video(mp4_file)
                
                if success:
                    log(f"✅ {mp4_file.name} 上传成功")
                    time.sleep(10)  # 视频间间隔
                    
                else:
                    # handle_merged_video 返回 False 有两种情况:
                    # 1. 配额耗尽 (已设置 LAST_QUOTA_EXHAUSTED_DATE)
                    # 2. 普通上传失败
                    
                    # 检查是否是配额问题
                    if YOUTUBE_ENABLE_QUOTA_MANAGEMENT and LAST_QUOTA_EXHAUSTED_DATE == today_str:
                        log("🛑 检测到配额耗尽,停止后续上传")
                        return  # 严重错误:立即退出
                    
                    # 普通失败:跳过并继续
                    log(f"⚠️  {mp4_file.name} 上传失败,跳过并继续下一个")
                    continue

            except HttpError as e:
                # HttpError 应该在 handle_merged_video 中被捕获
                # 如果到这里说明有漏网之鱼
                if e.resp.status == 403 and 'quotaExceeded' in str(e):
                    LAST_QUOTA_EXHAUSTED_DATE = get_today_utc_date_str()
                    log("🛑 检测到配额耗尽(顶层捕获),停止上传")
                    return  # 严重错误:立即退出
                else:
                    log(f"❌ {mp4_file.name} 发生HTTP错误: {e}")
                    send_upload_notification(mp4_file.name, "", False)
                    continue  # 一般错误:跳过并继续

            except Exception as e:
                # 捕获所有其他异常,防止整个进程崩溃
                log(f"❌ {mp4_file.name} 发生未知错误: {e}")
                import traceback
                log(f"详细堆栈:\n{traceback.format_exc()}")
                continue  # 一般错误:跳过并继续

        # ========== 4. 本轮处理完毕,重新扫描 ==========
        log("-" * 60)
        log("本轮处理完毕,重新扫描以检测新生成的视频...")
        log("")

    log("=" * 60)
    log("所有视频处理完毕")
    log("=" * 60)

def save_upload_info(file_path: Path, video_id: str, title: str, description: str, tags: list, upload_time: str):
    """保存上传信息到JSON文件"""
    from config import OUTPUT_DIR
    
    upload_info_file = OUTPUT_DIR / "recent_uploads.json"
    
    # 读取现有数据
    upload_data = {"uploads": []}
    if upload_info_file.exists():
        try:
            with open(upload_info_file, 'r', encoding='utf-8') as f:
                upload_data = json.load(f)
        except:
            upload_data = {"uploads": []}
    
    # 添加新的上传信息
    new_upload = {
        "filename": file_path.name,
        "video_id": video_id,
        "title": title,
        "description": description,
        "tags": tags,
        "upload_time": upload_time,
        "file_path": str(file_path)
    }
    
    upload_data["uploads"].insert(0, new_upload)  # 最新的在前面
    
    # 只保留最近50条记录
    upload_data["uploads"] = upload_data["uploads"][:50]
    
    # 保存到文件
    try:
        with open(upload_info_file, 'w', encoding='utf-8') as f:
            json.dump(upload_data, f, ensure_ascii=False, indent=2)
        if VERBOSE_LOGGING:
            log(f"上传信息已保存到: {upload_info_file}")
            publish_to_github_pages()
    except Exception as e:
        log(f"保存上传信息失败: {e}")

def main():
    """主函数，用于测试"""
    upload_all_pending_videos()

if __name__ == "__main__":
    main()