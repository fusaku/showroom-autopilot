# recorder/upscaler.py

import subprocess
import logging
import os
from pathlib import Path

# 配置
FFMPEG_OPTS = [
    "-c:v", "libx264",
    "-preset", "ultrafast",
    "-crf", "18",
    "-c:a", "copy",
    "-vf", "scale=1920:1080:flags=lanczos",
    "-threads", "0"
]

def upscale_file(input_path: Path, output_path: Path) -> bool:
    """
    调用 ffmpeg 将输入文件拉伸到 1080p
    安全策略：先输出到 .temp 文件，成功后再重命名。
    """
    if output_path.exists() and output_path.stat().st_size > 0:
        return True

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 定义临时文件
    temp_output_path = output_path.with_suffix(output_path.suffix + ".temp")

    if temp_output_path.exists():
        try:
            temp_output_path.unlink()
        except Exception:
            pass

    cmd = [
        "nice", "-n", "15",
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        "-i", str(input_path),
        *FFMPEG_OPTS,
        "-f", "mpegts",  # <--- 【关键修改】强制指定输出格式为 TS
        str(temp_output_path)
    ]

    try:
        logging.debug(f"🔥 开始拉伸: {input_path.name}")
        
        subprocess.run(cmd, check=True, timeout=600) 
        
        # 原子重命名
        os.rename(temp_output_path, output_path)
        
        # logging.debug(f"✅ 拉伸完成: {output_path.name}")
        return True

    except subprocess.TimeoutExpired:
        logging.error(f"❌ 拉伸超时: {input_path.name}")
        if temp_output_path.exists(): temp_output_path.unlink()
        return False
        
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ 拉伸失败: {input_path.name} - {e}")
        if temp_output_path.exists(): temp_output_path.unlink()
        return False
        
    except Exception as e:
        logging.error(f"❌ 未知错误: {e}")
        if temp_output_path.exists(): temp_output_path.unlink()
        return False