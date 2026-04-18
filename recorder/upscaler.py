# recorder/upscaler.py

import subprocess
import logging
import os
from pathlib import Path

def get_frame_rate(input_paths) -> str:
    if isinstance(input_paths, Path):
        input_paths = [input_paths]
    if not input_paths:
        return "40"
    
    mid = len(input_paths) // 2
    sample = input_paths[mid]
    
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=r_frame_rate,avg_frame_rate",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(sample)
        ], capture_output=True, text=True, timeout=30)
        
        lines = [l for l in result.stdout.strip().split('\n') if l and l != '0/0']
        
        rates = []
        for line in lines:
            if '/' in line:
                num, den = line.split('/')
                if int(den) > 0:
                    rates.append(int(num) // int(den))
        
        if rates:
            return str(max(rates))  # 取较大值，即 r_frame_rate
    except:
        pass
    return "40"

def upscale_file(input_path: Path, output_path: Path, fps: str = "40", is_filelist: bool = False) -> bool:
    """
    调用 ffmpeg 将输入文件拉伸到 1080p
    安全策略：先输出到 .temp 文件，成功后再重命名。
    """
    if output_path.exists() and output_path.stat().st_size > 0:
        return True

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 定义临时文件
    temp_output_path = output_path.with_suffix(".temp")

    if temp_output_path.exists():
        try:
            temp_output_path.unlink()
        except Exception:
            pass
    if is_filelist:
        input_args = ["-f", "concat", "-safe", "0", "-i", str(input_path)]
    else:
        input_args = ["-i", str(input_path)]
    cmd = [
        "nice", "-n", "15",
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
        *input_args,
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "18",
        "-c:a", "copy",
        "-vf", f"scale=1920:1080:flags=lanczos,fps={fps}",
        "-vsync", "cfr",
        "-f", "mp4",
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