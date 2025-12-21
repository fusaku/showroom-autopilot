import inspect
import sys
import os
from datetime import datetime
from pathlib import Path


def log(msg):
    frame = inspect.currentframe().f_back
    filename = frame.f_globals['__file__'].split('/')[-1]
    print(f"[{datetime.now().strftime('%m-%d %H:%M:%S')} - {filename}] {msg}")

# ========================= 路径配置 =========================
PARENT_DIR = Path("~/Downloads/Showroom/active").expanduser()  # 所有直播文件夹所在目录
OUTPUT_DIR = Path("~/Videos/merged").expanduser()  # 输出合并视频和日志的目录
VENV_ACTIVATE_DIR = "/home/ubuntu/venv" #python3 环境路径
# GitHub Pages 仓库路径
GITHUB_PAGES_REPO_PATH = Path("~/fusaku.github.io").expanduser()

# videos.json 文件路径
VIDEOS_JSON_PATH = GITHUB_PAGES_REPO_PATH / "videos.json"

# 字幕文件目标目录
SUBTITLES_TARGET_DIR = GITHUB_PAGES_REPO_PATH / "subtitles"

# 字幕文件源目录根路径
SUBTITLES_SOURCE_ROOT = Path("~/Downloads/Showroom").expanduser()

# 合并后的视频文件目录
try:
    from config import OUTPUT_DIR as MERGED_VIDEOS_DIR
except ImportError:
    MERGED_VIDEOS_DIR = Path("~/Videos/merged").expanduser()

# ==== 数据库配置 ====
# 认证信息文件的路径
CREDENTIALS_FILE = Path(__file__).parent / "db_credentials.key"

def load_db_credentials():
    """从 db_credentials.key 文件加载数据库用户名和密码"""
    
    if not CREDENTIALS_FILE.exists():
        print(f"错误: 找不到数据库凭证文件 {CREDENTIALS_FILE}", file=sys.stderr)
        sys.exit(1)
        
    try:
        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f.readlines()]
            if len(lines) < 2:
                print(f"错误: 凭证文件 {CREDENTIALS_FILE} 内容不足两行", file=sys.stderr)
                sys.exit(1)
                
            user = lines[0]
            password = lines[1]
            return user, password
            
    except Exception as e:
        print(f"错误: 读取凭证文件时出错: {e}", file=sys.stderr)
        sys.exit(1)

# 2. 调用函数来设置变量
DB_USER, DB_PASSWORD = load_db_credentials()

WALLET_DIR = "/home/ubuntu/Wallet_SRDB"
os.environ["TNS_ADMIN"] = WALLET_DIR
TNS_ALIAS = "srdb_high" 
DB_TABLE = "LIVE_STATUS" 
DB_HISTORY_TABLE = "SHOWROOM_LIVE_HISTORY"

# ========================= 检查配置 =========================
CHECK_INTERVAL = 15  # 每次检测间隔秒数
LIVE_INACTIVE_THRESHOLD = 60  # 判定直播结束的空闲秒数
MAX_WORKERS = 3  # 并发线程数
LIVE_CHECK_INTERVAL = 60  # 直播中检查文件的间隔秒数
MIN_FILES_FOR_CHECK = 5  # 开始检查的最小文件数量
FILE_STABLE_TIME = 5  # 文件稳定时间（秒），超过这个时间没修改的文件才检查
FINAL_INACTIVE_THRESHOLD = 60  # 1分钟文件无活动才确认结束（秒）

# ========================= 多文件夹处理配置 =========================
PROCESS_ALL_FOLDERS = True  # 是否处理所有文件夹（True）还是只处理最新的（False）
MAX_CONCURRENT_FOLDERS_PER_LIVE = 50  # 最大同时处理的文件夹数量（防止内存占用过多）
FOLDER_CLEANUP_DELAY = 120  # 完成的文件夹状态保留时间（秒），防止重复处理

# ========================= 字幕合并配置 =========================
SUBTITLE_ROOT = Path("/home/ubuntu/Downloads/Showroom").expanduser() # 字幕文件根目录
SUBTITLE_SUBPATH = "AKB48/comments"  # 日期目录下的子路径
TEMP_MERGED_DIR = PARENT_DIR / "temp_merged"  # 临时合并文件目录

# ==================== 字幕文件配置 ====================
# 日期格式（用于从文件名提取日期）
DATE_FORMAT_IN_FILENAME = "%y%m%d"

# ========================= 文件名配置 =========================
FILELIST_NAME = "filelist.txt"  # 文件列表文件名
LOG_SUFFIX = "_log.txt"  # 日志文件后缀
OUTPUT_EXTENSION = ".mp4"  # 输出视频文件扩展名

# ========================= FFmpeg 配置 =========================
FFMPEG_LOGLEVEL = "error"  # FFmpeg 日志级别 (quiet, panic, fatal, error, warning, info, verbose, debug)
FFMPEG_HIDE_BANNER = True  # 是否隐藏 FFmpeg banner

# ========================= FFprobe 配置 =========================
FFPROBE_TIMEOUT = 10  # FFprobe 检测超时时间（秒）

# ========================= 上传配置 =========================
ENABLE_AUTO_UPLOAD = True  # 是否启用自动上传功能

# ========================= 线程安全配置 =========================
LOCK_DIR = OUTPUT_DIR / ".locks"  # 锁文件目录
MERGE_LOCK_TIMEOUT = 300  # 合并锁超时时间（秒）
UPLOAD_LOCK_TIMEOUT = 600  # 上传锁超时时间（秒）

# ========================= 调试配置 =========================
DEBUG_MODE = True  # 调试模式，会输出更多信息
VERBOSE_LOGGING = True  # 详细日志模式

# ========================= YouTube API配置 =========================
# 认证文件路径
BASE_DIR = Path(__file__).parent.resolve()
# 主账号 (橋本陽菜)
YOUTUBE_CLIENT_SECRET_PATH = BASE_DIR / "credentials" / "autoupsr" / "client_secret.json"  # OAuth2客户端密钥文件
YOUTUBE_TOKEN_PATH = BASE_DIR / "credentials" / "autoupsr" / "youtube_token.pickle" # 访问令牌存储文件

# 副账号 (其他成员)
YOUTUBE_CLIENT_SECRET_PATH_ALT = BASE_DIR / "credentials" / "48g-SR" / "client_secret.json"
YOUTUBE_TOKEN_PATH_ALT = BASE_DIR / "credentials" / "48g-SR" / "youtube_token.pickle"

# API权限范围
YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

# ========================= YouTube上传配置 =========================
MEMBERS_YAML_PATH = BASE_DIR / "data" / "AKB48_members.yaml"  # 成员配置文件路径
# 视频默认设置
YOUTUBE_DEFAULT_TITLE = ""  # 默认标题（空字符串时使用文件名）
YOUTUBE_DEFAULT_DESCRIPTION = """
橋本陽菜
コメント付き：https://www.kg46.com
{upload_time}

#AKB48 #Team8 #橋本陽菜
""".strip()  # 默认描述，{upload_time}会被替换为上传时间

YOUTUBE_DEFAULT_TAGS = [
    "AKB48",
    "Team8", 
    "橋本陽菜",
    "Showroom",
]  # 默认标签

YOUTUBE_DEFAULT_CATEGORY_ID = "22"  # 默认分类ID (24=娱乐)
YOUTUBE_PRIVACY_STATUS = "public"  # 隐私状态: private, public, unlisted

# 播放列表配置
YOUTUBE_PLAYLIST_ID = ""  # 播放列表ID（空字符串表示不添加到播放列表）

# ========================= YouTube上传行为配置 =========================
YOUTUBE_UPLOAD_INTERVAL = 30  # YouTube上传检查间隔（秒）
YOUTUBE_RETRY_DELAY = 300  # 上传失败重试延迟（秒）
YOUTUBE_MAX_RETRIES = 3  # 最大重试次数

# 配额管理
YOUTUBE_QUOTA_RESET_HOUR_PACIFIC = 0  # 太平洋时间配额重置小时（0表示午夜）
YOUTUBE_ENABLE_QUOTA_MANAGEMENT = True  # 是否启用配额管理

# 上传完成后的行为
YOUTUBE_DELETE_AFTER_UPLOAD = False  # 上传成功后是否删除本地文件
YOUTUBE_MOVE_AFTER_UPLOAD = False  # 上传成功后是否移动文件到备份目录
YOUTUBE_BACKUP_DIR = OUTPUT_DIR / "uploaded_backup"  # 备份目录

# ========================= YouTube通知配置 =========================
YOUTUBE_ENABLE_NOTIFICATIONS = False  # 是否启用上传完成通知
YOUTUBE_NOTIFICATION_WEBHOOK_URL = ""  # Webhook通知URL（如Discord、Slack等）

# ==================== Git 配置 ====================

# 是否启用Git自动发布
ENABLE_GIT_AUTO_PUBLISH = True

# Git提交信息模板
GIT_COMMIT_MESSAGE_TEMPLATE = "Update videos and subtitles - {date} - {count} new videos"

# Git推送到的远程分支
GIT_REMOTE_BRANCH = "main"

# Git操作超时时间（秒）
GIT_TIMEOUT = 300

# 是否在Git操作前先拉取最新代码
GIT_PULL_BEFORE_PUSH = True

# ==================== 发布行为配置 ====================

# 是否在上传完成后自动发布到GitHub Pages
ENABLE_AUTO_PUBLISH_AFTER_UPLOAD = True

# 每次发布后的延迟时间（秒）
PUBLISH_DELAY_SECONDS = 30

# ==================== 错误处理配置 ====================

# 遇到错误时是否继续处理其他文件
CONTINUE_ON_ERROR = True

# 最大重试次数（Git操作）
MAX_RETRY_ATTEMPTS = 3

# ==================== 字幕文件配置 ====================

# 支持的字幕文件扩展名
SUBTITLE_EXTENSIONS = ['.ass']

# 【新增】字幕时间轴偏移量（秒）。正数表示字幕时间向后延迟。
# 根据您的计算，这里设置为 10 秒。
SUBTITLE_OFFSET_SECONDS = 15