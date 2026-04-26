# recorder/upscaler.py

import subprocess
import logging
import os
from pathlib import Path
import time

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
    加强版：带中间过程日志与稳定性预处理
    """
    if output_path.exists() and output_path.stat().st_size > 0:
        return True

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path = output_path.with_suffix(".temp")
    
    temp_combined_path = None
    actual_input = input_path

    try:
        # --- 步骤 1: 预处理 (仅针对 TS 列表) ---
        if is_filelist:
            temp_combined_path = output_path.parent / f"pre_merge_{int(time.time())}.mp4"
            logging.info(f"🔄 [1/2 预处理] 正在合并片段以稳定时间轴: {input_path.name}")
            
            merge_cmd = [
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                "-f", "concat", "-safe", "0", "-i", str(input_path),
                "-c", "copy", "-movflags", "+faststart", 
                str(temp_combined_path)
            ]
            
            start_merge = time.time()
            subprocess.run(merge_cmd, check=True, timeout=300)
            logging.info(f"✅ [1/2 预处理] 合并成功，耗时 {time.time() - start_merge:.2f}s")
            actual_input = temp_combined_path

        # --- 步骤 2: 正式拉伸 ---
        if temp_output_path.exists():
            temp_output_path.unlink()

        logging.info(f"🔥 [2/2 拉伸中] 正在进行 1080p 编码: {output_path.name}")
        
        cmd = [
            "nice", "-n", "15",
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-fflags", "+genpts", # 强制重新生成时间戳，双保险
            "-i", str(actual_input),
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-crf", "18",
            "-c:a", "copy",
            "-vf", f"scale=1920:1080:flags=lanczos,fps={fps}",
            "-vsync", "cfr",
            "-f", "mp4",
            str(temp_output_path)
        ]

        start_upscale = time.time()
        subprocess.run(cmd, check=True, timeout=600) 
        
        # 原子重命名
        os.rename(temp_output_path, output_path)
        logging.info(f"✨ [任务完成] 成功产出: {output_path.name}，编码耗时 {time.time() - start_upscale:.2f}s")
        return True

    except subprocess.CalledProcessError as e:
        # 捕捉 FFmpeg 报错日志
        logging.error(f"❌ [FFmpeg 报错] 任务 {input_path.name} 失败。返回码: {e.returncode}")
        if temp_output_path.exists(): temp_output_path.unlink()
        return False
        
    except Exception as e:
        logging.error(f"❌ [未知错误] {str(e)}")
        if temp_output_path.exists(): temp_output_path.unlink()
        return False
        
    finally:
        # --- 步骤 3: 清理 ---
        if temp_combined_path and temp_combined_path.exists():
            try:
                temp_combined_path.unlink()
                logging.debug(f"🧹 已清理临时中间文件")
            except Exception as e:
                logging.warning(f"⚠️ 清理临时文件失败: {e}")