#!/usr/bin/env python3
"""
实例管理工具 - Instance Manager
管理检测器和录制器实例，包括注册、状态更新、负载查看等

使用示例:
    # 查看所有实例
    python manage_instances.py --list
    
    # 注册新实例
    python manage_instances.py --register monitor-a monitor "检测器A" --capacity 100
    python manage_instances.py --register recorder-a recorder "录制器A(主机1)" --capacity 10
    
    # 更新实例状态
    python manage_instances.py --status monitor-a active
    python manage_instances.py --status recorder-a inactive  # 标记为已停用
    
    # 查看负载分布
    python manage_instances.py --load
    
    # 更新心跳
    python manage_instances.py --heartbeat monitor-a
    
    # 删除实例
    python manage_instances.py --delete recorder-old
"""

import argparse
import os
import sys
import cx_Oracle
from datetime import datetime
from tabulate import tabulate
from pathlib import Path

# 添加当前目录到路径
script_dir = Path(__file__).resolve().parent
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

# 导入数据库配置
try:
    from config import DB_USER, DB_PASSWORD, TNS_ALIAS, WALLET_DIR
except ImportError:
    print("错误: 无法导入config.py，请确保配置文件存在")
    sys.exit(1)


def get_connection():
    """获取数据库连接"""
    os.environ["TNS_ADMIN"] = WALLET_DIR
    return cx_Oracle.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        dsn=TNS_ALIAS,
        encoding="UTF-8",
        nencoding="UTF-8"
    )


def list_instances():
    """列出所有实例"""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                INSTANCE_ID,
                INSTANCE_TYPE,
                DISPLAY_NAME,
                MAX_CAPACITY,
                CURRENT_LOAD,
                STATUS,
                CONFIG_VERSION,
                LAST_HEARTBEAT,
                UPDATED_AT
            FROM ADMIN.V_INSTANCE_LOAD
            ORDER BY INSTANCE_TYPE, INSTANCE_ID
        """)
        
        results = cursor.fetchall()
        
        if results:
            headers = ["实例ID", "类型", "显示名", "容量", "负载", "状态", "配置版本", "最后心跳", "创建时间"]
            print(f"\n实例列表 (共 {len(results)} 个):\n")
            
            # 格式化数据
            formatted_results = []
            for row in results:
                formatted_row = list(row)
                # 格式化类型
                formatted_row[1] = '检测器' if row[1] == 'monitor' else '录制器'
                # 格式化状态
                status_map = {'active': '运行中', 'inactive': '已停用', 'maintenance': '维护中'}
                formatted_row[5] = status_map.get(row[5], row[5])
                # 格式化时间
                if row[7]:  # LAST_HEARTBEAT
                    formatted_row[7] = row[7].strftime('%Y-%m-%d %H:%M:%S')
                formatted_row[8] = row[8].strftime('%Y-%m-%d %H:%M:%S')
                formatted_results.append(formatted_row)
            
            print(tabulate(formatted_results, headers=headers, tablefmt="simple"))
        else:
            print("没有找到任何实例")


def show_load():
    """显示负载分布"""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                INSTANCE_ID,
                INSTANCE_TYPE,
                DISPLAY_NAME,
                MAX_CAPACITY,
                CURRENT_LOAD,
                AVAILABLE_CAPACITY,
                LOAD_PERCENT,
                STATUS
            FROM ADMIN.V_INSTANCE_LOAD
            ORDER BY INSTANCE_TYPE, LOAD_PERCENT DESC
        """)
        
        results = cursor.fetchall()
        
        if results:
            headers = ["实例ID", "类型", "显示名", "最大容量", "当前负载", "剩余容量", "负载率%", "状态"]
            print("\n实例负载统计:\n")
            
            # 格式化数据
            formatted_results = []
            for row in results:
                formatted_row = list(row)
                # 格式化类型
                formatted_row[1] = '检测器' if row[1] == 'monitor' else '录制器'
                # 格式化状态
                status_map = {'active': '✓运行', 'inactive': '✗停用', 'maintenance': '⚠维护'}
                formatted_row[7] = status_map.get(row[7], row[7])
                # 添加负载条
                load_percent = row[6] if row[6] else 0
                bar_length = int(load_percent / 5)  # 每5%一个符号
                formatted_row.append('█' * bar_length)
                formatted_results.append(formatted_row)
            
            headers.append("负载可视化")
            print(tabulate(formatted_results, headers=headers, tablefmt="simple"))
        else:
            print("没有找到任何实例")


def register_instance(instance_id, instance_type, display_name, capacity, host_info=None):
    """注册新实例"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # 检查是否已存在
        cursor.execute("""
            SELECT COUNT(*) FROM ADMIN.INSTANCES WHERE INSTANCE_ID = :instance_id
        """, instance_id=instance_id)
        
        if cursor.fetchone()[0] > 0:
            print(f"✗ 实例 {instance_id} 已存在")
            return
        
        # 插入新实例
        cursor.execute("""
            INSERT INTO ADMIN.INSTANCES (
                INSTANCE_ID, INSTANCE_TYPE, DISPLAY_NAME, MAX_CAPACITY, HOST_INFO, STATUS
            ) VALUES (
                :instance_id, :instance_type, :display_name, :capacity, :host_info, 'active'
            )
        """, {
            'instance_id': instance_id,
            'instance_type': instance_type,
            'display_name': display_name,
            'capacity': capacity,
            'host_info': host_info
        })
        
        conn.commit()
        print(f"✓ 成功注册实例: {instance_id} ({display_name})")


def update_status(instance_id, status):
    """更新实例状态"""
    valid_status = ['active', 'inactive', 'maintenance']
    if status not in valid_status:
        print(f"✗ 无效的状态: {status}，有效值: {', '.join(valid_status)}")
        return
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE ADMIN.INSTANCES
            SET STATUS = :status, CONFIG_VERSION = CONFIG_VERSION + 1
            WHERE INSTANCE_ID = :instance_id
        """, {
            'status': status,
            'instance_id': instance_id
        })
        
        if cursor.rowcount > 0:
            conn.commit()
            status_map = {'active': '运行中', 'inactive': '已停用', 'maintenance': '维护中'}
            print(f"✓ 已更新实例 {instance_id} 状态为: {status_map[status]}")
            print(f"  配置版本号已自增，实例将在下次轮询时获取新状态")
        else:
            print(f"✗ 未找到实例: {instance_id}")


def update_heartbeat(instance_id):
    """更新心跳时间"""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE ADMIN.INSTANCES
            SET LAST_HEARTBEAT = CURRENT_TIMESTAMP
            WHERE INSTANCE_ID = :instance_id
        """, instance_id=instance_id)
        
        if cursor.rowcount > 0:
            conn.commit()
            print(f"✓ 已更新实例 {instance_id} 心跳时间")
        else:
            print(f"✗ 未找到实例: {instance_id}")


def delete_instance(instance_id, force=False):
    """删除实例"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # 检查是否有分配的成员
        cursor.execute("""
            SELECT COUNT(*) FROM ADMIN.MEMBER_INSTANCES 
            WHERE INSTANCE_ID = :instance_id
        """, instance_id=instance_id)
        
        count = cursor.fetchone()[0]
        
        if count > 0 and not force:
            print(f"✗ 实例 {instance_id} 还有 {count} 个成员分配")
            print(f"  请先迁移成员，或使用 --force 强制删除")
            return
        
        # 删除实例（会级联删除 MEMBER_INSTANCES）
        cursor.execute("""
            DELETE FROM ADMIN.INSTANCES WHERE INSTANCE_ID = :instance_id
        """, instance_id=instance_id)
        
        if cursor.rowcount > 0:
            conn.commit()
            print(f"✓ 已删除实例: {instance_id}")
            if count > 0:
                print(f"  同时删除了 {count} 个成员分配记录")
        else:
            print(f"✗ 未找到实例: {instance_id}")


def show_instance_members(instance_id):
    """显示实例分配的成员"""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                m.MEMBER_ID,
                m.NAME_EN,
                m.NAME_JP,
                g.NAME as GROUP_NAME,
                m.TEAM,
                mi.ENABLED,
                mi.PRIORITY,
                mi.ASSIGNED_BY,
                mi.ASSIGNED_AT
            FROM ADMIN.MEMBER_INSTANCES mi
            JOIN ADMIN.MEMBERS m ON mi.MEMBER_ID = m.ID
            JOIN ADMIN.GROUPS g ON m.GROUP_ID = g.ID
            WHERE mi.INSTANCE_ID = :instance_id
            ORDER BY mi.PRIORITY DESC, m.NAME_EN
        """, instance_id=instance_id)
        
        results = cursor.fetchall()
        
        if results:
            headers = ["成员ID", "英文名", "日文名", "组合", "Team", "启用", "优先级", "分配方式", "分配时间"]
            print(f"\n实例 {instance_id} 的成员分配 (共 {len(results)} 个):\n")
            
            formatted_results = []
            for row in results:
                formatted_row = list(row)
                formatted_row[5] = '✓' if row[5] == 1 else '✗'
                formatted_row[8] = row[8].strftime('%Y-%m-%d %H:%M:%S')
                formatted_results.append(formatted_row)
            
            print(tabulate(formatted_results, headers=headers, tablefmt="simple"))
        else:
            print(f"实例 {instance_id} 没有分配任何成员")


def show_assignment_history(instance_id=None, member_id=None, limit=50):
    """显示分配历史"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        sql = """
            SELECT 
                mih.MEMBER_ID,
                m.NAME_EN,
                mih.INSTANCE_ID,
                mih.INSTANCE_TYPE,
                mih.ACTION,
                mih.OLD_INSTANCE_ID,
                mih.REASON,
                mih.OPERATED_BY,
                mih.OPERATED_AT
            FROM ADMIN.MEMBER_INSTANCES_HISTORY mih
            LEFT JOIN ADMIN.MEMBERS m ON mih.MEMBER_ID = m.ID
            WHERE 1=1
        """
        
        params = {}
        
        if instance_id:
            sql += " AND (mih.INSTANCE_ID = :instance_id OR mih.OLD_INSTANCE_ID = :instance_id)"
            params['instance_id'] = instance_id
        
        if member_id:
            sql += " AND mih.MEMBER_ID = (SELECT ID FROM ADMIN.MEMBERS WHERE MEMBER_ID = :member_id)"
            params['member_id'] = member_id
        
        sql += " ORDER BY mih.OPERATED_AT DESC FETCH FIRST :limit ROWS ONLY"
        params['limit'] = limit
        
        cursor.execute(sql, params)
        results = cursor.fetchall()
        
        if results:
            headers = ["成员ID", "成员名", "实例ID", "类型", "操作", "原实例", "原因", "操作者", "时间"]
            print(f"\n分配历史记录 (最近 {len(results)} 条):\n")
            
            formatted_results = []
            for row in results:
                formatted_row = list(row)
                # 格式化类型
                formatted_row[3] = '检测' if row[3] == 'monitor' else '录制'
                # 格式化操作
                action_map = {
                    'assigned': '分配',
                    'removed': '移除',
                    'disabled': '禁用',
                    'enabled': '启用',
                    'migrated': '迁移'
                }
                formatted_row[4] = action_map.get(row[4], row[4])
                # 格式化时间
                formatted_row[8] = row[8].strftime('%Y-%m-%d %H:%M:%S')
                formatted_results.append(formatted_row)
            
            print(tabulate(formatted_results, headers=headers, tablefmt="simple"))
        else:
            print("没有找到历史记录")


def main():
    parser = argparse.ArgumentParser(description='实例管理工具')
    
    # 查询操作
    parser.add_argument('--list', action='store_true', help='列出所有实例')
    parser.add_argument('--load', action='store_true', help='显示负载分布')
    parser.add_argument('--members', type=str, metavar='INSTANCE_ID', help='显示实例的成员分配')
    parser.add_argument('--history', nargs='?', const='ALL', metavar='INSTANCE_ID', help='显示分配历史')
    
    # 实例管理
    parser.add_argument('--register', nargs=3, metavar=('INSTANCE_ID', 'TYPE', 'NAME'), 
                       help='注册新实例 (类型: monitor/recorder)')
    parser.add_argument('--capacity', type=int, help='实例容量（与--register配合使用）')
    parser.add_argument('--host', type=str, help='主机信息（与--register配合使用）')
    
    parser.add_argument('--status', nargs=2, metavar=('INSTANCE_ID', 'STATUS'), 
                       help='更新实例状态 (active/inactive/maintenance)')
    parser.add_argument('--heartbeat', type=str, metavar='INSTANCE_ID', help='更新心跳时间')
    parser.add_argument('--delete', type=str, metavar='INSTANCE_ID', help='删除实例')
    parser.add_argument('--force', action='store_true', help='强制删除（即使有成员分配）')
    
    args = parser.parse_args()
    
    # 如果没有参数，显示帮助
    if not any(vars(args).values()):
        parser.print_help()
        return
    
    try:
        if args.list:
            list_instances()
        
        if args.load:
            show_load()
        
        if args.members:
            show_instance_members(args.members)
        
        if args.history:
            if args.history == 'ALL':
                show_assignment_history()
            else:
                show_assignment_history(instance_id=args.history)
        
        if args.register:
            instance_id, instance_type, display_name = args.register
            capacity = args.capacity if args.capacity else 100
            register_instance(instance_id, instance_type, display_name, capacity, args.host)
        
        if args.status:
            instance_id, status = args.status
            update_status(instance_id, status)
        
        if args.heartbeat:
            update_heartbeat(args.heartbeat)
        
        if args.delete:
            delete_instance(args.delete, args.force)
    
    except Exception as e:
        print(f"✗ 错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()