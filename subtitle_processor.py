import re
from pathlib import Path
from typing import Callable, Optional

# ASS 字幕时间戳格式: H:MM:SS.cs (小时:分钟:秒.百分之一秒)
# 修正: 使用 \d+ 而不是 \d 来匹配多位小时数
ASS_TIME_RE = re.compile(
    r"Dialogue: \d+,(?P<start_time>\d+:\d{2}:\d{2}\.\d{2}),(?P<end_time>\d+:\d{2}:\d{2}\.\d{2}),"
)


def _time_to_centiseconds(ass_time: str) -> int:
    """将 ASS 时间格式 (H:MM:SS.cs) 转换为以 0.01 秒为单位的整数 (厘秒)"""
    try:
        parts = ass_time.split(':')
        
        if len(parts) != 3:
            raise ValueError(f"时间格式不正确,应为 H:MM:SS.cs")
        
        # 提取小时、分钟、秒和厘秒
        hours = int(parts[0])
        minutes = int(parts[1])
        
        seconds_cs = parts[2].split('.')
        if len(seconds_cs) != 2:
            raise ValueError(f"秒和厘秒格式不正确,应为 SS.cs")
            
        seconds = int(seconds_cs[0])
        centiseconds = int(seconds_cs[1])
        
        # 验证范围
        if not (0 <= minutes < 60) or not (0 <= seconds < 60) or not (0 <= centiseconds < 100):
            raise ValueError(f"时间值超出有效范围")
        
        # 转换为厘秒
        total_centiseconds = (hours * 3600 + minutes * 60 + seconds) * 100 + centiseconds
        return total_centiseconds
        
    except (IndexError, ValueError) as e:
        raise ValueError(f"无法解析 ASS 时间格式 '{ass_time}': {e}")


def _centiseconds_to_time(centiseconds: int) -> str:
    """将以 0.01 秒为单位的整数 (厘秒) 转换回 ASS 时间格式 (H:MM:SS.cs)"""
    
    # 确保时间不为负值
    if centiseconds < 0:
        centiseconds = 0 
    
    hours = centiseconds // 360000
    centiseconds %= 360000
    
    minutes = centiseconds // 6000
    centiseconds %= 6000
    
    seconds = centiseconds // 100
    centiseconds %= 100
    
    # 格式化输出为 H:MM:SS.cs
    return f"{hours}:{minutes:02}:{seconds:02}.{centiseconds:02}"


def offset_subtitle(
    source_path: Path, 
    offset_seconds: int, 
    log_func: Callable[[str, str], None]
) -> Optional[Path]:
    """
    读取 ASS 字幕文件,应用时间轴偏移,并将结果写入临时文件。

    :param source_path: 原始字幕文件的路径。
    :param offset_seconds: 要向后延迟的秒数（正整数）。
    :param log_func: 用于记录日志的函数。
    :return: 临时文件的路径 (Path),如果失败则返回 None。
    """
    
    # 验证输入
    if not source_path.exists():
        log_func(f"文件不存在: {source_path}", "ERROR")
        return None
        
    if source_path.suffix.lower() != '.ass':
        log_func(f"不是 ASS 格式文件: {source_path.name}", "WARN")
        return None

    offset_centiseconds = offset_seconds * 100
    
    # 临时文件放在源文件的父目录
    temp_path = source_path.parent / f"temp_processed_{source_path.name}"
    
    processed_lines = 0
    skipped_lines = 0
    
    try:
        with source_path.open('r', encoding='utf-8') as infile, \
             temp_path.open('w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile, 1):
                match = ASS_TIME_RE.match(line)
                
                if match:
                    # 匹配到对话行,进行时间偏移
                    try:
                        start_time_str = match.group('start_time')
                        end_time_str = match.group('end_time')

                        # 1. 转换为厘秒并应用偏移
                        start_cs = _time_to_centiseconds(start_time_str) + offset_centiseconds
                        end_cs = _time_to_centiseconds(end_time_str) + offset_centiseconds
                        
                        # 2. 转换回 ASS 格式字符串
                        new_start_time = _centiseconds_to_time(start_cs)
                        new_end_time = _centiseconds_to_time(end_cs)
                        
                        # 3. 替换行中的时间戳
                        new_line = line.replace(start_time_str, new_start_time, 1)
                        new_line = new_line.replace(end_time_str, new_end_time, 1)
                        outfile.write(new_line)
                        processed_lines += 1
                        
                    except ValueError as e:
                        # 单行解析失败,记录错误但继续处理
                        log_func(
                            f"跳过无法解析的对话行 (行{line_num}): {line.strip()[:50]}... - {e}", 
                            "WARN"
                        )
                        outfile.write(line)  # 保留原始行
                        skipped_lines += 1
                else:
                    # 非对话行（如 [Script Info], [V4+ Styles]）直接复制
                    outfile.write(line)
        
        log_func(
            f"字幕处理完成: {source_path.name} -> {temp_path.name} "
            f"(偏移{offset_seconds}秒, 处理{processed_lines}行, 跳过{skipped_lines}行)", 
            "INFO"
        )
        return temp_path

    except Exception as e:
        log_func(f"处理字幕文件 {source_path.name} 时发生错误: {e}", "ERROR")
        # 清理失败的临时文件
        if temp_path.exists():
            try:
                temp_path.unlink()
            except:
                pass
        return None