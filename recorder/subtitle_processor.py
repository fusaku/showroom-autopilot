import re
import logging
import json
import math
from pathlib import Path
from typing import Optional

# ASS 字幕时间戳格式: H:MM:SS.cs
ASS_TIME_RE = re.compile(
    r"Dialogue: \d+,(?P<start_time>\d+:\d{2}:\d{2}\.\d{2}),(?P<end_time>\d+:\d{2}:\d{2}\.\d{2}),"
)

def _time_to_centiseconds(ass_time: str) -> int:
    """将 ASS 时间格式 (H:MM:SS.cs) 转换为厘秒整数"""
    try:
        parts = ass_time.split(':')
        if len(parts) != 3:
            raise ValueError("时间格式不正确")
        
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds_cs = parts[2].split('.')
        seconds = int(seconds_cs[0])
        centiseconds = int(seconds_cs[1])
        
        return (hours * 3600 + minutes * 60 + seconds) * 100 + centiseconds
    except (IndexError, ValueError) as e:
        raise ValueError(f"无法解析时间 '{ass_time}': {e}")

def _centiseconds_to_time(centiseconds: int) -> str:
    """将厘秒整数转换回 ASS 时间格式"""
    if centiseconds < 0:
        centiseconds = 0 
    
    hours = centiseconds // 360000
    centiseconds %= 360000
    minutes = centiseconds // 6000
    centiseconds %= 6000
    seconds = centiseconds // 100
    centiseconds %= 100
    
    return f"{hours}:{minutes:02}:{seconds:02}.{centiseconds:02}"

def offset_subtitle(
    source_path,  # Path 或 list[Path]
    offset_seconds: int
) -> Optional[Path]:
    """
    应用时间轴偏移
    支持:
    - 单个 ASS 文件
    - 单个 JSON 文件  
    - 多个 JSON 文件列表 (自动合并)
    """
    # 1. 判断是列表还是单个文件
    if isinstance(source_path, list):
        if len(source_path) == 0:
            logging.error("JSON 文件列表为空")
            return None
        elif len(source_path) == 1:
            source_path = source_path[0]  # 单个文件,直接处理
        else:
            # 多个 JSON,先合并
            logging.info(f"检测到 {len(source_path)} 个 JSON 文件,开始合并")
            merged_json = _merge_json_files(source_path)
            if not merged_json:
                return None
            source_path = merged_json
    
    if not source_path.exists():
        logging.error(f"文件不存在: {source_path}")
        return None

    # 新增: 如果是 JSON,先生成 ASS
    if source_path.suffix.lower() == '.json':
        logging.info(f"检测到 JSON 文件,转换为 ASS: {source_path.name}")
        ass_path = _generate_ass_from_json(source_path)
        if not ass_path:
            return None
        source_path = ass_path  # 替换为生成的 ASS

    if source_path.suffix.lower() != '.ass':
        logging.warning(f"不是 ASS 格式文件: {source_path.name}")
        return None

    offset_centiseconds = offset_seconds * 100
    temp_path = source_path.parent / f"temp_processed_{source_path.name}"
    
    processed_lines = 0
    skipped_lines = 0
    
    try:
        with source_path.open('r', encoding='utf-8') as infile, \
             temp_path.open('w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile, 1):
                match = ASS_TIME_RE.match(line)
                if match:
                    try:
                        start_time_str = match.group('start_time')
                        end_time_str = match.group('end_time')

                        start_cs = _time_to_centiseconds(start_time_str) + offset_centiseconds
                        end_cs = _time_to_centiseconds(end_time_str) + offset_centiseconds
                        
                        new_line = line.replace(start_time_str, _centiseconds_to_time(start_cs), 1)
                        new_line = new_line.replace(end_time_str, _centiseconds_to_time(end_cs), 1)
                        outfile.write(new_line)
                        processed_lines += 1
                    except ValueError as e:
                        logging.warning(f"跳过行 {line_num}: {e}")
                        outfile.write(line)
                        skipped_lines += 1
                else:
                    outfile.write(line)
        
        # 使用 info 记录处理结果
        logging.info(f"字幕处理完成: {source_path.name} (偏移{offset_seconds}s)")
        return temp_path

    except Exception as e:
        logging.error(f"处理字幕文件失败 {source_path.name}: {e}")
        if temp_path.exists():
            temp_path.unlink()
        return None
def _generate_ass_from_json(json_path: Path) -> Optional[Path]:
    """从 comments.json 生成 ASS 字幕文件"""
    try:
        # 读取 JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            comment_log = json.load(f)
        
        if len(comment_log) == 0:
            logging.warning(f"JSON 文件为空: {json_path.name}")
            return None
        
        # 获取开始时间 (第一条消息的时间戳,毫秒)
        ws_startTime = comment_log[0]['received_at']
        
        # 生成 ASS 内容
        ass_text = _convert_comments_to_danmaku(
            ws_startTime,
            comment_log,
            fontsize=18,
            fontname='MS PGothic',
            alpha='1A',
            width=640,
            height=360
        )
        
        # 保存 ASS 文件 (和 JSON 同名,只改扩展名)
        ass_path = json_path.with_suffix('.ass')
        with open(ass_path, 'w', encoding='utf-8') as f:
            f.write(ass_text)
        
        logging.info(f"成功生成 ASS 文件: {ass_path.name}")
        return ass_path
        
    except Exception as e:
        logging.error(f"从 JSON 生成 ASS 失败 {json_path.name}: {e}")
        return None
def _merge_json_files(json_files: list) -> Optional[Path]:
    """合并多个 JSON 字幕文件"""
    try:
        all_comments = []
        
        for json_file in json_files:
            logging.debug(f"读取: {json_file.name}")
            comments = _load_json_with_repair(json_file)
            
            if comments:
                all_comments.extend(comments)
                logging.debug(f"  → {len(comments)} 条评论")
            else:
                logging.warning(f"  → 跳过 (空或损坏)")
        
        if not all_comments:
            logging.error("所有 JSON 文件都无法读取")
            return None
        
        # 按接收时间排序
        all_comments.sort(key=lambda x: x.get('received_at', 0))
        
        # 去重
        unique_comments = []
        seen = set()
        for comment in all_comments:
            key = (comment.get('received_at', 0), comment.get('cm', ''))
            if key not in seen:
                seen.add(key)
                unique_comments.append(comment)
        
        # 保存到临时文件
        merged_filename = f"merged_{json_files[0].stem}.json"
        merged_path = json_files[0].parent / merged_filename
        
        with open(merged_path, 'w', encoding='utf-8') as f:
            json.dump(unique_comments, f, indent=2, ensure_ascii=False)
        
        logging.info(f"✅ 合并完成: {len(json_files)} 个文件 → {len(unique_comments)} 条评论 (去重后)")
        
        return merged_path
        
    except Exception as e:
        logging.error(f"合并 JSON 文件失败: {e}")
        return None


def _load_json_with_repair(json_file: Path) -> list:
    """加载 JSON,如果损坏尝试修复"""
    try:
        # 先尝试正常加载
        with open(json_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logging.warning(f"JSON 文件损坏: {json_file.name}, 尝试修复...")
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 方案 A: 找最后一个完整对象
            last_complete = content.rfind('},')
            if last_complete == -1:
                last_complete = content.rfind('}')
            
            if last_complete == -1:
                logging.error(f"无法修复: {json_file.name}")
                return []
            
            # 截取并补上 ]
            repaired = content[:last_complete + 1] + '\n]'
            repaired_data = json.loads(repaired)
            
            logging.warning(f"✅ 修复成功: {json_file.name}, 保留 {len(repaired_data)} 条")
            
            return repaired_data
            
        except Exception as repair_error:
            logging.error(f"修复失败: {json_file.name} - {repair_error}")
            return []

def _convert_comments_to_danmaku(startTime, commentList,
                                 fontsize=18, fontname='MS PGothic', alpha='1A',
                                 width=640, height=360):
    """
    将评论转换为弹幕字幕
    (从 showroom comments.py 复制)
    """
    def msecToAssTime(msf):
        """毫秒转 ASS 时间格式"""
        sec, msf = divmod(msf, 1000)
        minute, sec = divmod(sec, 60)
        hour, minute = divmod(minute, 60)
        return f'{hour}:{minute:02}:{sec:02}.{msf // 10:02}'
    
    # 屏幕上最大弹幕行数
    slotsNum = math.floor(height / fontsize)
    travelTime = 8 * 1000  # 8秒飞行时间
    
    # ASS 文件头部
    danmaku = "[Script Info]\n"
    danmaku += "ScriptType: v4.00+\n"
    danmaku += "Collisions: Normal\n"
    danmaku += f"PlayResX: {width}\n"
    danmaku += f"PlayResY: {height}\n\n"
    danmaku += "[V4+ Styles]\n"
    danmaku += "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n"
    danmaku += f"Style: danmakuFont, {fontname}, {fontsize}, &H00FFFFFF, &H00FFFFFF, &H00000000, &H00000000, 1, 0, 0, 0, 100, 100, 0.00, 0.00, 1, 1, 0, 2, 20, 20, 20, 0\n\n"
    danmaku += "[Events]\n"
    danmaku += "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
    
    # 弹幕槽位
    slots = [0] * slotsNum
    previousTelop = ''
    
    for data in commentList:
        m_type = str(data['t'])
        comment = ''
        
        if m_type == '1':  # 评论
            comment = data['cm']
            
        elif m_type == '3':  # 投票开始
            poll = data.get('l', [])
            if len(poll) < 1:
                continue
            comment = 'Poll Started: 【({})'.format(poll[0]['id'] % 10000)
            for k in range(1, min(len(poll), 5)):
                comment += ', ({})'.format(poll[k]['id'] % 10000)
            if len(poll) > 5:
                comment += ', ...'
            comment += '】'
            
        elif m_type == '4':  # 投票结果
            poll = data.get('l', [])
            if len(poll) < 1:
                continue
            comment = 'Poll: 【({}) {}%'.format(poll[0]['id'] % 10000, poll[0]['r'])
            for k in range(1, min(len(poll), 5)):
                comment += ', ({}) {}%'.format(poll[k]['id'] % 10000, poll[k]['r'])
            if len(poll) > 5:
                comment += ', ...'
            comment += '】'
            
        elif m_type == '8':  # telop
            telop = data.get('telop')
            if telop is not None and telop != previousTelop:
                previousTelop = telop
                comment = 'Telop: 【' + telop + '】'
            else:
                continue
                
        else:
            continue
        
        # 计算相对时间
        t = data['received_at'] - startTime
        
        # 查找可用槽位
        selectedSlot = 0
        isSlotFound = False
        for j in range(slotsNum):
            if slots[j] <= t:
                slots[j] = t + travelTime
                isSlotFound = True
                selectedSlot = j
                break
        
        # 所有槽位都满,找最早结束的
        if not isSlotFound:
            minIdx = 0
            for j in range(1, slotsNum):
                if slots[j] < slots[minIdx]:
                    minIdx = j
            slots[minIdx] = t + travelTime
            selectedSlot = minIdx
        
        # 计算弹幕位置
        y1 = fontsize * selectedSlot + fontsize
        y2 = y1
        x1 = width + len(comment) * fontsize
        x2 = 0 - len(comment) * fontsize
        
        # 生成 ASS 字幕行
        sub = f"Dialogue: 3,{msecToAssTime(t)},{msecToAssTime(t + travelTime)},"
        sub += f"danmakuFont,,0000,0000,0000,,{{\\alpha&H{alpha}&\\move("
        sub += f"{x1},{y1},{x2},{y2})}}{comment}\n"
        
        danmaku += sub
    
    return danmaku