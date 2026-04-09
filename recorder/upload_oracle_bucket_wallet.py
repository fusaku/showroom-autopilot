#!/usr/bin/env python3
"""
Oracle对象存储上传模块 (使用数据库Wallet认证)
无需额外配置API密钥,直接复用数据库Wallet
"""

import oci
import logging
import sys
import subprocess
from pathlib import Path
from datetime import datetime

# ============================================================
# 初始化日志系统 (必须在导入config之前)
# ============================================================
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
sys.path.insert(0, str(_project_root / "shared"))
sys.path.insert(0, str(_project_root / "recorder"))

from logger_config import setup_logger
setup_logger()

# 导入项目配置
from config import (
    OUTPUT_DIR, 
    MERGED_VIDEOS_DIR,
    BUCKET_NAMESPACE,
    BUCKET_NAME,
    BUCKET_REGION,
    BUCKET_PREFIX,
    USE_INSTANCE_PRINCIPAL,
    BUCKET_DELETE_AFTER_UPLOAD,
    BUCKET_CREATE_UPLOAD_MARKER,
    BUCKET_UPLOAD_MEMBER_FILTER
)

# ============================================================
# 上传功能
# ============================================================

class OracleBucketUploader:
    """Oracle对象存储上传器 (使用Wallet认证)"""
    
    def __init__(self):
        """初始化上传器"""
        try:
            if USE_INSTANCE_PRINCIPAL:
                # 方式1: 使用实例主体认证 (推荐,无需配置)
                # 如果你的服务器实例有对象存储权限,直接用这个
                try:
                    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
                    self.client = oci.object_storage.ObjectStorageClient(
                        config={},
                        signer=signer
                    )
                    logging.info("✅ 使用实例主体认证")
                except Exception as e:
                    logging.warning(f"实例主体认证失败: {e}")
                    # 方式2: 使用资源主体认证
                    signer = oci.auth.signers.get_resource_principals_signer()
                    self.client = oci.object_storage.ObjectStorageClient(
                        config={},
                        signer=signer
                    )
                    logging.info("✅ 使用资源主体认证")
            else:
                # 使用标准配置文件认证 (需要 ~/.oci/config)
                config = oci.config.from_file()
                self.client = oci.object_storage.ObjectStorageClient(config)
                logging.info("✅ 使用配置文件认证")
            
        except Exception as e:
            logging.error(f"❌ Oracle对象存储客户端初始化失败: {e}")
            logging.error("请确保:")
            logging.error("1. 实例有对象存储访问权限 (动态组策略)")
            logging.error("2. 或者已配置 ~/.oci/config 文件")
            raise
    
    def upload_file(self, video_path: Path) -> bool:
        """
        上传单个视频文件到对象存储
        
        参数:
            video_path: 视频文件路径
        
        返回:
            True: 上传成功
            False: 上传失败
        """
        try:
            # 构造对象名称 (保留文件名)
            object_name = f"{BUCKET_PREFIX}{video_path.name}"
            
            logging.info(f"📤 开始上传: {video_path.name}")
            logging.debug(f"   目标: {BUCKET_NAMESPACE}/{BUCKET_NAME}/{object_name}")
            
            # 执行上传
            with open(video_path, 'rb') as file_data:
                self.client.put_object(
                    namespace_name=BUCKET_NAMESPACE,
                    bucket_name=BUCKET_NAME,
                    object_name=object_name,
                    put_object_body=file_data
                )
            
            logging.info(f"✅ 上传成功: {video_path.name}")
            
            # 上传对应的 .uploaded 标记文件
            uploaded_marker = video_path.with_suffix('.mp4.uploaded')
            if uploaded_marker.exists():
                try:
                    marker_object_name = f"{BUCKET_PREFIX}{uploaded_marker.name}"
                    with open(uploaded_marker, 'rb') as marker_data:
                        self.client.put_object(
                            namespace_name=BUCKET_NAMESPACE,
                            bucket_name=BUCKET_NAME,
                            object_name=marker_object_name,
                            put_object_body=marker_data
                        )
                    logging.info(f"✅ 已上传标记文件: {uploaded_marker.name}")
                except Exception as e:
                    logging.warning(f"⚠️  标记文件上传失败: {e}")

            # 创建上传标记
            if BUCKET_CREATE_UPLOAD_MARKER:
                marker_file = video_path.parent / f"{video_path.stem}.uploaded_bucket"
                marker_file.write_text(
                    f"Uploaded: {datetime.now()}\n"
                    f"Bucket: {BUCKET_NAMESPACE}/{BUCKET_NAME}\n"
                    f"Object: {object_name}\n"
                )
            
            return True
            
        except Exception as e:
            logging.error(f"❌ 上传失败 {video_path.name}: {e}")
            return False
    
    def upload_pending_videos(self) -> int:
        """
        扫描并上传所有待上传的视频

        返回:
            成功上传的视频数量
        """
        success_count = 0

        # 扫描所有已合并但未上传的视频
        for video_file in MERGED_VIDEOS_DIR.glob("*.mp4"):
            # 过滤:只上传包含成员名的视频
            if BUCKET_UPLOAD_MEMBER_FILTER and BUCKET_UPLOAD_MEMBER_FILTER not in video_file.name:
                logging.debug(f"⏭️  跳过(不匹配): {video_file.name}")
                continue
            
            # 检查是否已上传
            uploaded_marker = video_file.parent / f"{video_file.stem}.uploaded_bucket"
            if uploaded_marker.exists():
                logging.debug(f"⏭️  已上传,跳过: {video_file.name}")
                continue

            is_high_quality = False
            try:
                # 快速检测分辨率高度
                cmd = ["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=height", "-of", "csv=p=0", str(video_file)]
                height = int(subprocess.check_output(cmd).decode().strip())
                if height > 720:
                    is_high_quality = True
            except Exception:
                pass # 检测失败当作低画质处理，直接上传不等待

            # 只有当：是高清视频(>720p) 且 没有YouTube上传标记 时，才跳过
            youtube_marker = video_file.with_suffix('.mp4.uploaded')
            if is_high_quality and not youtube_marker.exists():
                logging.debug(f"⏭️  跳过(1080p+ 等待YouTube上传): {video_file.name}")
                continue

            # 执行上传
            if self.upload_file(video_file):
                success_count += 1

        logging.info(f"📊 本次上传完成: {success_count} 个视频")
        return success_count


# ============================================================
# 公共接口
# ============================================================

def upload_to_oracle_bucket(video_path: Path) -> bool:
    """
    上传单个视频到Oracle对象存储
    
    参数:
        video_path: 视频文件路径
    
    返回:
        True: 上传成功
        False: 上传失败
    """
    try:
        uploader = OracleBucketUploader()
        return uploader.upload_file(video_path)
    except Exception as e:
        logging.error(f"上传失败: {e}")
        return False


def upload_all_pending_to_bucket():
    """
    扫描并上传所有待上传的视频
    
    返回:
        成功上传的视频数量
    """
    try:
        uploader = OracleBucketUploader()
        return uploader.upload_pending_videos()
    except Exception as e:
        logging.error(f"批量上传失败: {e}")
        return 0


# ============================================================
# 命令行入口
# ============================================================

def main():
    """独立运行时的入口函数"""
   
    success_count = upload_all_pending_to_bucket()
    
    if success_count > 0:
        logging.info(f"✅ 上传完成: {success_count} 个视频")
    else:
        logging.info("ℹ️  没有待上传的视频")


if __name__ == "__main__":
    main()