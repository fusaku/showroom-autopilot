"""
数据库成员配置加载器
替代YAML文件,从Oracle数据库加载成员配置

使用方法:
在 config.py 中替换 load_members() 函数为:
    from db_members_loader import load_members_from_db as load_members
"""

import os
import sys
import json
import time
import logging
import cx_Oracle
from pathlib import Path
from typing import List, Dict, Tuple, Optional

# ============================================
# 确保能找到config.py (修复导入问题)
# ============================================
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

# 数据库连接配置 - 从config导入
try:
    from config import DB_USER, DB_PASSWORD, TNS_ALIAS, WALLET_DIR
except ImportError:
    # 如果无法导入,使用默认值
    DB_USER = None
    DB_PASSWORD = None
    TNS_ALIAS = None
    WALLET_DIR = None

# 全局连接池
_db_pool = None

def get_db_pool():
    """获取或创建数据库连接池(单例模式)"""
    global _db_pool
    
    if _db_pool is None:
        try:
            os.environ["TNS_ADMIN"] = WALLET_DIR
            _db_pool = cx_Oracle.SessionPool(
                user=DB_USER,
                password=DB_PASSWORD,
                dsn=TNS_ALIAS,
                min=1,
                max=5,
                increment=1,
                encoding="UTF-8",
                nencoding="UTF-8",
                threaded=True
            )
            logging.info("✓ 数据库连接池初始化成功")
        except Exception as e:
            logging.error(f"✗ 数据库连接池初始化失败: {e}")
            return None
    
    return _db_pool


def load_members_from_db() -> Tuple[List[Dict], Dict]:
    """
    从数据库加载成员配置
    
    返回:
        (all_enabled_members, global_templates)
        - all_enabled_members: 启用的成员列表
        - global_templates: 全局模板字典(暂时为空,保持兼容性)
    """
    all_enabled_members = []
    global_templates = {}
    
    pool = get_db_pool()
    if not pool:
        logging.error("无法连接数据库,返回空成员列表")
        return all_enabled_members, global_templates
    
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                # 查询所有启用的成员及其完整配置
                cursor.execute("""
                    SELECT 
                        m.MEMBER_ID,
                        m.ROOM_ID,
                        m.NAME_JP,
                        m.NAME_EN,
                        m.TEAM,
                        m.ROOM_URL_KEY,
                        g.NAME as GROUP_NAME,
                        yc.TITLE_TEMPLATE,
                        yc.DESCRIPTION_TEMPLATE,
                        yc.CATEGORY_ID,
                        yc.PRIVACY_STATUS,
                        yc.PLAYLIST_ID,
                        yc.USE_PRIMARY_ACCOUNT
                    FROM ADMIN.MEMBERS m
                    JOIN ADMIN.GROUPS g ON m.GROUP_ID = g.ID
                    LEFT JOIN ADMIN.YOUTUBE_CONFIGS yc ON m.ID = yc.MEMBER_ID
                    WHERE m.ENABLED = 1
                    ORDER BY g.NAME, m.NAME_EN
                """)
                
                members_data = cursor.fetchall()
                
                for row in members_data:
                    member_db_id_query = """
                        SELECT ID FROM ADMIN.MEMBERS WHERE MEMBER_ID = :member_id
                    """
                    cursor.execute(member_db_id_query, {'member_id': row[0]})
                    member_db_id = cursor.fetchone()[0]
                    
                    # 获取tags
                    cursor.execute("""
                        SELECT TAG 
                        FROM ADMIN.YOUTUBE_TAGS
                        WHERE MEMBER_ID = :member_id
                        ORDER BY SORT_ORDER
                    """, {'member_id': member_db_id})
                    
                    tags = [tag[0] for tag in cursor.fetchall()]
                    
                    # 读取CLOB字段
                    title_template = row[7].read() if row[7] else ''
                    description_template = row[8].read() if row[8] else ''
                    
                    # 构建成员字典(与YAML格式完全一致)
                    member = {
                        'id': row[0],
                        'room_id': row[1],
                        'name_jp': row[2],
                        'name_en': row[3],
                        'team': row[4],
                        'enabled': True,  # 查询已经过滤了enabled=1
                        'room_url_key': row[5],
                        'youtube': {
                            'title_template': title_template,
                            'description_template': description_template,
                            'tags': tags,
                            'category_id': row[9] or '22',
                            'privacy_status': row[10] or 'public',
                            'playlist_id': row[11] or '',
                            'use_primary_account': bool(row[12])
                        }
                    }
                    
                    all_enabled_members.append(member)
                
                logging.info(f"✓ 从数据库加载了 {len(all_enabled_members)} 个启用的成员")
    
    except Exception as e:
        logging.error(f"✗ 从数据库加载成员失败: {e}")
        import traceback
        traceback.print_exc()
    
    return all_enabled_members, global_templates


def get_enabled_members() -> List[Dict]:
    """
    获取启用的成员列表(兼容旧代码)
    
    返回:
        启用的成员列表
    """
    members, _ = load_members_from_db()
    return members


# 模块级缓存,避免每次都查数据库
_cached_members = None
_cache_timestamp = 0
_cache_ttl = 60  # 缓存60秒


def load_members_from_db_cached() -> Tuple[List[Dict], Dict]:
    """
    从数据库加载成员配置(带缓存)
    
    返回:
        (all_enabled_members, global_templates)
    """
    global _cached_members, _cache_timestamp
    
    import time
    current_time = time.time()
    
    # 如果缓存有效,直接返回
    if _cached_members is not None and (current_time - _cache_timestamp) < _cache_ttl:
        return _cached_members
    
    # 否则重新加载
    result = load_members_from_db()
    _cached_members = result
    _cache_timestamp = current_time
    
    return result


# 为了方便,提供一个刷新缓存的函数
def refresh_members_cache():
    """强制刷新成员缓存"""
    global _cached_members, _cache_timestamp
    _cached_members = None
    _cache_timestamp = 0
    return load_members_from_db_cached()


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.INFO)
    
    print("测试从数据库加载成员...")
    members, templates = load_members_from_db()
    
    print(f"\n找到 {len(members)} 个启用的成员:")
    for member in members[:5]:  # 只显示前5个
        print(f"  - {member['name_en']} ({member['name_jp']}) - {member['team']}")
        if 'youtube' in member:
            print(f"    YouTube标签: {', '.join(member['youtube']['tags'][:3])}")
    
    if len(members) > 5:
        print(f"  ... 还有 {len(members) - 5} 个成员")