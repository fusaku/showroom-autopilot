#!/usr/bin/env python3
"""
负载均衡器模块
供 monitor_showroom.py 调用，在发现新直播时分配录制器
"""

import logging
import cx_Oracle
from datetime import datetime


class LoadBalancer:
    """负载均衡器：将新直播分配给负载最小的录制器"""
    
    def __init__(self, db_connection):
        """
        初始化负载均衡器
        
        参数:
            db_connection: 数据库连接对象
        """
        self.conn = db_connection
    
    def assign_recorder(self, member_id: str) -> str:
        """
        为直播成员分配录制器
        
        参数:
            member_id: 成员ID (字符串，如 'okabe_rin')
        
        返回:
            分配的录制器实例ID (如 'recorder-a')，失败返回 None
        """
        try:
            cursor = self.conn.cursor()
            
            # 1. 获取成员的数据库ID
            cursor.execute("""
                SELECT ID FROM ADMIN.MEMBERS WHERE MEMBER_ID = :member_id
            """, {'member_id': member_id})
            
            result = cursor.fetchone()
            if not result:
                logging.error(f"未找到成员: {member_id}")
                cursor.close()
                return None
            
            member_db_id = result[0]
            
            # 2. 检查是否已经分配（避免重复分配）
            cursor.execute("""
                SELECT INSTANCE_ID 
                FROM ADMIN.MEMBER_INSTANCES
                WHERE MEMBER_ID = :member_id
                  AND INSTANCE_TYPE = 'recorder'
                  AND ENABLED = 1
            """, {'member_id': member_db_id})
            
            existing = cursor.fetchone()
            if existing:
                # 已经分配过了，直接返回
                recorder_id = existing[0]
                logging.debug(f"{member_id} 已分配给 {recorder_id}")
                cursor.close()
                return recorder_id
            
            # 3. 查询所有可用录制器及其当前负载
            cursor.execute("""
                SELECT 
                    i.INSTANCE_ID,
                    COUNT(mi.ID) as load_count
                FROM ADMIN.INSTANCES i
                LEFT JOIN ADMIN.MEMBER_INSTANCES mi 
                    ON i.INSTANCE_ID = mi.INSTANCE_ID 
                    AND mi.INSTANCE_TYPE = 'recorder'
                    AND mi.ENABLED = 1
                WHERE i.INSTANCE_TYPE = 'recorder'
                  AND i.STATUS = 'active'
                GROUP BY i.INSTANCE_ID
                ORDER BY load_count ASC
            """)
            
            recorders = cursor.fetchall()
            
            if not recorders:
                logging.error("没有可用的录制器实例！")
                cursor.close()
                return None
            
            # 4. 选择负载最小的录制器
            recorder_id = recorders[0][0]
            current_load = recorders[0][1]
            
            # 5. 写入分配
            cursor.execute("""
                INSERT INTO ADMIN.MEMBER_INSTANCES (
                    MEMBER_ID, 
                    INSTANCE_ID, 
                    INSTANCE_TYPE, 
                    ENABLED, 
                    ASSIGNED_BY
                ) VALUES (
                    :member_id, 
                    :instance_id, 
                    'recorder', 
                    1, 
                    'auto-on-live'
                )
            """, {
                'member_id': member_db_id,
                'instance_id': recorder_id
            })
            
            self.conn.commit()
            
            logging.info(f"✓ {member_id} → {recorder_id} (负载: {current_load})")
            
            cursor.close()
            return recorder_id
            
        except cx_Oracle.IntegrityError as e:
            # 唯一约束冲突，说明已经分配过了
            self.conn.rollback()
            logging.debug(f"{member_id} 分配时发生约束冲突（可能已分配）: {e}")
            return None
        except Exception as e:
            self.conn.rollback()
            logging.error(f"分配录制器失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def clear_assignment(self, member_id: str):
        """
        清除成员的录制器分配（直播结束时调用）
        
        参数:
            member_id: 成员ID (字符串)
        """
        try:
            cursor = self.conn.cursor()
            
            # 删除自动分配的记录
            cursor.execute("""
                DELETE FROM ADMIN.MEMBER_INSTANCES
                WHERE MEMBER_ID = (
                    SELECT ID FROM ADMIN.MEMBERS WHERE MEMBER_ID = :member_id
                )
                AND INSTANCE_TYPE = 'recorder'
                AND ASSIGNED_BY = 'auto-on-live'
            """, {'member_id': member_id})
            
            deleted_count = cursor.rowcount
            
            if deleted_count > 0:
                self.conn.commit()
                logging.debug(f"✓ 清除 {member_id} 的录制器分配")
            
            cursor.close()
            
        except Exception as e:
            self.conn.rollback()
            logging.error(f"清除分配失败: {e}")
            import traceback
            traceback.print_exc()
    
    def get_assignment(self, member_id: str) -> str:
        """
        查询成员当前分配的录制器
        
        参数:
            member_id: 成员ID
        
        返回:
            录制器实例ID，未分配返回 None
        """
        try:
            cursor = self.conn.cursor()
            
            cursor.execute("""
                SELECT mi.INSTANCE_ID
                FROM ADMIN.MEMBER_INSTANCES mi
                JOIN ADMIN.MEMBERS m ON mi.MEMBER_ID = m.ID
                WHERE m.MEMBER_ID = :member_id
                  AND mi.INSTANCE_TYPE = 'recorder'
                  AND mi.ENABLED = 1
            """, {'member_id': member_id})
            
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] if result else None
            
        except Exception as e:
            logging.error(f"查询分配失败: {e}")
            return None


# 测试代码
if __name__ == "__main__":
    import os
    import sys
    from pathlib import Path
    
    # 添加父目录到路径
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from config import get_db_connection
    
    logging.basicConfig(level=logging.INFO)
    
    conn = get_db_connection()
    if not conn:
        print("无法连接数据库")
        sys.exit(1)
    
    balancer = LoadBalancer(conn)
    
    # 测试分配
    print("\n测试1: 分配录制器")
    recorder_id = balancer.assign_recorder('okabe_rin')
    print(f"分配结果: {recorder_id}")
    
    # 测试查询
    print("\n测试2: 查询分配")
    assigned = balancer.get_assignment('okabe_rin')
    print(f"当前分配: {assigned}")
    
    # 测试清除
    print("\n测试3: 清除分配")
    balancer.clear_assignment('okabe_rin')
    
    # 再次查询
    print("\n测试4: 清除后查询")
    assigned = balancer.get_assignment('okabe_rin')
    print(f"当前分配: {assigned}")
    
    conn.close()
    print("\n测试完成")