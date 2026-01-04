import logging
import sys
import os
import yaml
import cx_Oracle
from datetime import datetime
from pathlib import Path


# def init_global_logging():
#     log_format = "[%(levelname)-5s - %(asctime)s - %(filename)s] %(message)s"
#     date_format = "%m-%d %H:%M:%S"
#     formatter = logging.Formatter(log_format, datefmt=date_format)
    
#     console_handler = logging.StreamHandler(sys.stdout)
#     console_handler.setFormatter(formatter)
    
#     root_logger = logging.getLogger()
#     root_logger.setLevel(logging.INFO)
#     root_logger.addHandler(console_handler)

# init_global_logging()  # 立即执行

# ============================================================
# 1. 基础路径与全局变量
# ============================================================
LOG_DIR = Path("~/logs").expanduser()
# 确保目录存在
LOG_DIR.mkdir(exist_ok=True)
BASE_DIR = Path(__file__).resolve().parent
PARENT_DIR = Path("~/Downloads/Showroom/active").expanduser()  # 所有直播文件夹所在目录
TS_PARENT_DIR = Path("~/Downloads/Showroom/active").expanduser() # 保留原有变量
OUTPUT_DIR = Path("/mnt/video/merged").expanduser()  # 输出合并视频和日志的目录
VENV_ACTIVATE_DIR = "/home/ubuntu/venv" #python3 环境路径
sys.path.insert(0, str(BASE_DIR))

# 锁与调试
LOCK_DIR = OUTPUT_DIR / ".locks"
DEBUG_MODE = True
VERBOSE_LOGGING = True

# ============================================================
# 2. 数据库配置 (Oracle SRDB)
# ============================================================
# 修复: 先定义凭证文件路径和加载函数
CREDENTIALS_FILE = Path(__file__).resolve().parent / "db_credentials.key"

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
            return lines[0], lines[1]
    except Exception as e:
        print(f"错误: 读取凭证文件时出错: {e}", file=sys.stderr)
        sys.exit(1)

# 修复: 先定义DB_USER等变量
DB_USER, DB_PASSWORD = load_db_credentials()

WALLET_DIR = "/home/ubuntu/Wallet_SRDB"
os.environ["TNS_ADMIN"] = WALLET_DIR
TNS_ALIAS = "srdb_high" 
DB_TABLE = "LIVE_STATUS" 
DB_HISTORY_TABLE = "SHOWROOM_LIVE_HISTORY"

# ============================================================
# 3. 录制与扫描逻辑 (检查间隔/并发控制)
# ============================================================
CHECK_INTERVAL = 30  # 每次检测间隔秒数
LIVE_INACTIVE_THRESHOLD = 60  # 判定直播结束的空闲秒数
MAX_WORKERS = 16  # 并发线程数
LIVE_CHECK_INTERVAL = 90  # 直播中检查文件的间隔秒数
MIN_FILES_FOR_CHECK = 5  # 开始检查的最小文件数量
FILE_STABLE_TIME = 5  # 文件稳定时间（秒），超过这个时间没修改的文件才检查
FINAL_INACTIVE_THRESHOLD = 60  # 1分钟文件无活动才确认结束（秒）

# 多文件夹处理配置
PROCESS_ALL_FOLDERS = True  # 是否处理所有文件夹（True）还是只处理最新的（False）
MAX_CONCURRENT_FOLDERS_PER_LIVE = 50  # 最大同时处理的文件夹数量（防止内存占用过多）
FOLDER_CLEANUP_DELAY = 120  # 完成的文件夹状态保留时间（秒），防止重复处理

# ============================================================
# 4. 视频合并与字幕配置 (FFmpeg/FFprobe)
# ============================================================
FILELIST_NAME = "filelist.txt"
LOG_SUFFIX = "_log.txt"
OUTPUT_EXTENSION = ".mp4"
MERGE_LOCK_TIMEOUT = 300

FFMPEG_LOGLEVEL = "error"
FFMPEG_HIDE_BANNER = True
FFPROBE_TIMEOUT = 3

# 字幕合并配置
TEMP_MERGED_DIR = PARENT_DIR / "temp_merged"  # 临时合并文件目录
DATE_FORMAT_IN_FILENAME = "%y%m%d"
SUBTITLE_EXTENSIONS = ['.ass']
SUBTITLE_OFFSET_SECONDS = 12 # 字幕时间轴偏移量（秒）

# 【补全】字幕源路径与目标路径
SUBTITLES_SOURCE_ROOT = Path("~/Downloads/Showroom").expanduser() # 字幕文件源目录根路径
MERGED_VIDEOS_DIR = OUTPUT_DIR  # 使用OUTPUT_DIR,避免循环导入

# 阈值常量
INITIAL_MATCH_THRESHOLD = 60      # 第一个 JSON 匹配阈值
CONTINUATION_THRESHOLD = 180      # 后续 JSON 链接阈值
# ==========================================================
# 5. YouTube API 与 成员配置
# ============================================================
MEMBERS_DATA_DIR = BASE_DIR / "data"

# def load_all_members_configs():
#     """遍历目录加载所有yaml并合并"""
#     all_members = []
#     if not MEMBERS_DATA_DIR.exists():
#         return all_members
#     for yaml_file in MEMBERS_DATA_DIR.glob("*.y*ml"):
#         try:
#             with open(yaml_file, 'r', encoding='utf-8') as f:
#                 data = yaml.safe_load(f)
#                 if data and 'members' in data:
#                     all_members.extend(data['members'])
#         except Exception as e:
#             logging.error(f"加载配置文件 {yaml_file.name} 失败: {e}")
#     return all_members

YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

# 主账号 (橋本陽菜)
YOUTUBE_CLIENT_SECRET_PATH = BASE_DIR / "credentials" / "autoupsr" / "client_secret.json"
YOUTUBE_TOKEN_PATH = BASE_DIR / "credentials" / "autoupsr" / "youtube_token.pickle"

# 副账号 (其他成员)
YOUTUBE_CLIENT_SECRET_PATH_ALT = BASE_DIR / "credentials" / "48g-SR" / "client_secret.json"
YOUTUBE_TOKEN_PATH_ALT = BASE_DIR / "credentials" / "48g-SR" / "youtube_token.pickle"

# 第三个账号配置（用于非AKB48成员）
YOUTUBE_CLIENT_SECRET_PATH_THIRD = BASE_DIR / "credentials" / "idol-SR" / "client_secret.json"
YOUTUBE_TOKEN_PATH_THIRD = BASE_DIR / "credentials" / "idol-SR" / "youtube_token.pickle"

# ============================================================
# 6. YouTube 上传行为与默认参数
# ============================================================
ENABLE_AUTO_UPLOAD = True
UPLOAD_LOCK_TIMEOUT = 600
YOUTUBE_UPLOAD_INTERVAL = 30
YOUTUBE_RETRY_DELAY = 300
YOUTUBE_MAX_RETRIES = 3

# 配额管理
YOUTUBE_ENABLE_QUOTA_MANAGEMENT = True
YOUTUBE_QUOTA_RESET_HOUR_PACIFIC = 0 

# 上传默认模板
YOUTUBE_PRIVACY_STATUS = "public"
YOUTUBE_DEFAULT_CATEGORY_ID = "22"
YOUTUBE_PLAYLIST_ID = ""
YOUTUBE_DEFAULT_TITLE = ""
YOUTUBE_DEFAULT_DESCRIPTION = """
橋本陽菜
コメント付き：https://www.kg46.com
{upload_time}

#AKB48 #Team8 #橋本陽菜
""".strip()
YOUTUBE_DEFAULT_TAGS = ["AKB48", "Team8", "橋本陽菜", "Showroom"]

# 后序处理
YOUTUBE_DELETE_AFTER_UPLOAD = False
YOUTUBE_MOVE_AFTER_UPLOAD = False
YOUTUBE_BACKUP_DIR = OUTPUT_DIR / "uploaded_backup"
YOUTUBE_ENABLE_NOTIFICATIONS = False
YOUTUBE_NOTIFICATION_WEBHOOK_URL = ""

# ============================================================
# 7. GitHub Pages 与 Git 发布配置
# ============================================================
GITHUB_PAGES_REPO_PATH = Path("~/fusaku.github.io").expanduser()
VIDEOS_JSON_PATH = GITHUB_PAGES_REPO_PATH / "videos.json"
SUBTITLES_TARGET_DIR = GITHUB_PAGES_REPO_PATH / "subtitles" # 字幕文件目标目录

ENABLE_GIT_AUTO_PUBLISH = True
ENABLE_AUTO_PUBLISH_AFTER_UPLOAD = True
GIT_REMOTE_BRANCH = "main"
GIT_PULL_BEFORE_PUSH = True
GIT_TIMEOUT = 300
PUBLISH_DELAY_SECONDS = 30
GIT_COMMIT_MESSAGE_TEMPLATE = "Update videos and subtitles - {date} - {count} new videos"

# 错误控制
CONTINUE_ON_ERROR = True
MAX_RETRY_ATTEMPTS = 3


# ============================================================
# 8. 状态监控配置 (State-detection专用)
# ============================================================

# ========================= 1. 基础路径与全局配置 =========================


# ========================= 2. 多IP配置 =========================
OUTBOUND_IPS = [
    "10.0.0.114", "10.0.0.46", "10.0.0.175", "10.0.0.140", "10.0.0.160",
    "10.0.0.116", "10.0.0.57", "10.0.0.174", "10.0.0.222", "10.0.0.150",
    "10.0.0.60", "10.0.0.61", "10.0.0.62", "10.0.0.63", "10.0.0.64",
    "10.0.0.65", "10.0.0.66", "10.0.0.67", "10.0.0.68", "10.0.0.69",
    "10.0.0.70", "10.0.0.71", "10.0.0.72", "10.0.0.73", "10.0.0.74",
    "10.0.0.75", "10.0.0.76", "10.0.0.77", "10.0.0.78", "10.0.0.79"
]

# ========================= 3. 监控配置 =========================
MAX_TS_INACTIVE_TIME = 120      # TS文件停止更新判定阈值
GRACEFUL_START_DELAY = 6       # 启动缓冲
FILE_CHECK_GRACE_PERIOD = 35    # smart-start 启动宽限期
STOP_DELAY = 300               # 停止录制延迟

REQUEST_INTERVAL = 5
MIN_RESTART_INTERVAL = 30
RESTART_CHECK_INTERVAL = 3
FILE_INACTIVITY_THRESHOLD = 120 #文件不活动触发重启的阈值（秒）
SHOWROOM_SCRIPT_DIR = Path("/home/ubuntu/showroom")
SHOWROOM_SCRIPT_PATH = SHOWROOM_SCRIPT_DIR / "showroom.py"

# ============================================================
# 9. 数据库辅助函数
# ============================================================
def get_db_connection():
    """每个脚本调用此函数获取属于自己的独立连接"""
    os.environ["TNS_ADMIN"] = WALLET_DIR
    try:
        conn = cx_Oracle.connect(user=DB_USER, password=DB_PASSWORD, dsn=TNS_ALIAS,
                                encoding="UTF-8", nencoding="UTF-8")
        return conn
    except Exception as e:
        logging.error(f"数据库连接失败: {e}")
        return None

def check_db_alive(conn):
    """通用的连接活性检查"""
    if not conn: return False
    try:
        conn.ping()
        return True
    except:
        return False

# ============================================================
# 10. 成员配置加载 (放在最后,避免循环导入)
# ============================================================
# 修复: 在所有必需的变量定义之后才导入db_members_loader
def _load_members_lazy():
    from db_members_loader import load_members_from_db_cached
    return load_members_from_db_cached()

_members_list, _templates_dict = _load_members_lazy()
ENABLED_MEMBERS = _members_list
GLOBAL_TEMPLATES = _templates_dict

def get_enabled_members():
    """获取启用的成员列表(兼容旧代码)"""
    return ENABLED_MEMBERS