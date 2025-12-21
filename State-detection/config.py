import sys
import os
import yaml
from pathlib import Path

# ==== 多IP配置 ====
OUTBOUND_IPS = [
    "10.0.0.114",  # 例如: "10.0.0.100"
    "10.0.0.46",
    "10.0.0.175",
    "10.0.0.140",  # 例如: "10.0.0.101"
    "10.0.0.160",
    "10.0.0.116",
    "10.0.0.57",
    "10.0.0.174",
    "10.0.0.222",
    "10.0.0.150",
    # 可以继续添加更多IP
]
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

# ==== 路径配置 ====
TS_PARENT_DIR = Path("~/Downloads/Showroom/active").expanduser()
LOG_DIR = Path("~/logs").expanduser()

# 创建必要的目录
LOG_DIR.mkdir(exist_ok=True)

# ==== 成员配置 ====
MEMBERS_FILE = Path(__file__).parent.parent / "data" / "AKB48_members.yaml"

def load_members():
    """从 AKB48_members.yaml 加载成员配置"""
    if not MEMBERS_FILE.exists():
        print(f"错误: 找不到 {MEMBERS_FILE}")
        sys.exit(1)
    
    with open(MEMBERS_FILE, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    
    # 只返回 enabled=True 的成员
    enabled = [m for m in data.get("members", []) if m.get("enabled", False)]
    
    if not enabled:
        print("错误: AKB48_members.yaml 中没有启用的成员")
        sys.exit(1)
    
    return enabled

def get_enabled_members():
    """获取最新的成员配置"""
    return load_members()

ENABLED_MEMBERS = load_members()

# ==== 监控配置 ====
REQUEST_INTERVAL = max(0.5, 30 / len(ENABLED_MEMBERS))  # 每个请求之间的间隔秒数
MIN_RESTART_INTERVAL = 30  # 最小重启间隔（秒）
RESTART_CHECK_INTERVAL = 1  # 重启检查间隔（秒），即多久扫描一次数据库
# ==== Streamlink启动配置 ====
GRACEFUL_START_DELAY = 6