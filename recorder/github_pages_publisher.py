"""
GitHub Pages视频发布脚本
功能：更新videos.json、移动字幕文件、Git上传
"""

import json
import shutil
import time
import subprocess
import re
import subtitle_processor
import logging
import sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "shared"))
from typing import List, Dict, Optional, Tuple
from config import *

# ==================== 验证配置 ====================

def validate_config():
    """验证配置的有效性"""
    errors = []
    
    if not SUBTITLES_SOURCE_ROOT.exists():
        errors.append(f"字幕源目录不存在: {SUBTITLES_SOURCE_ROOT}")
    
    if not MERGED_VIDEOS_DIR.exists():
        errors.append(f"视频目录不存在: {MERGED_VIDEOS_DIR}")
    
    if not GITHUB_PAGES_REPO_PATH.exists():
        errors.append(f"GitHub Pages仓库路径不存在: {GITHUB_PAGES_REPO_PATH}")
    elif not (GITHUB_PAGES_REPO_PATH / ".git").exists():
        errors.append(f"指定路径不是Git仓库: {GITHUB_PAGES_REPO_PATH}")
    
    if errors:
        logging.error("配置验证失败:")
        for error in errors:
            logging.error(f"  - {error}")
        return False
    
    return True

def ensure_directories():
    """确保必要的目录存在"""
    SUBTITLES_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    logging.debug(f"确保目录存在: {SUBTITLES_TARGET_DIR}")

class GitHubPagesPublisher:
    """GitHub Pages视频发布器"""
    
    def __init__(self):
        """初始化发布器"""
        if not validate_config():
            raise ValueError("配置验证失败")
        
        ensure_directories()
        
        self.stats = {
            'processed_videos': 0,
            'new_videos': 0,
            'moved_subtitles': 0,
            'errors': []
        }
    
        self._video_cache = None
    
    def extract_date_from_filename(self, filename: str) -> Optional[str]:
        """从文件名提取日期 (例如: 250808 -> 2025-08-08)"""
        try:
            date_match = re.match(r'^(\d{6})', filename)
            if date_match:
                date_str = date_match.group(1)
                parsed_date = datetime.strptime(date_str, DATE_FORMAT_IN_FILENAME)
                return parsed_date.strftime("%Y-%m-%d")
        except Exception as e:
            logging.warning(f"从文件名提取日期失败 {filename}: {e}")
        return None
    
    def get_video_id_from_uploaded_flag(self, video_file: Path) -> Optional[str]:
        """从.uploaded标记文件获取视频ID"""
        uploaded_flag = video_file.with_suffix(video_file.suffix + ".uploaded")
        
        if not uploaded_flag.exists():
            return None
        
        try:
            # 如果.uploaded文件中存储了视频ID
            if uploaded_flag.stat().st_size > 0:
                with open(uploaded_flag, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    # YouTube视频ID是11个字符
                    if len(content) == 11 and re.match(r'^[a-zA-Z0-9_-]+$', content):
                        return content
            
            logging.error(f"无法从上传标记获取视频ID: {video_file.name}")
            return None
            
        except Exception as e:
            logging.error(f"读取上传标记失败 {video_file.name}: {e}")
            return None
    
    def find_subtitle_files(self, video_file: Path) -> list:
        """查找所有匹配的 JSON 字幕文件,返回路径列表"""
        video_filename = video_file.stem
        
        # 1. 解析视频文件的日期、名字和时间戳 (参考 checker.py 逻辑)
        pattern = r'^(\d{6})\s+Showroom\s+-\s+(.+?)\s+(\d{6})$'
        match = re.match(pattern, video_filename)
        if not match:
            return []  # 返回空列表

        v_date, v_name, v_time_str = match.groups()
        v_time = int(v_time_str)

        # 第一步: 找第一个匹配的 JSON (60秒内,取最接近的)
        first_json = None
        min_diff = 999999

        for sub_file in SUBTITLES_SOURCE_ROOT.rglob("*comments.json"):
            sub_name = sub_file.stem

            if v_date in sub_name and v_name in sub_name:
                sub_time_match = re.search(r'(\d{6})', sub_name.replace(v_date, "", 1))

                if sub_time_match:
                    s_time = int(sub_time_match.group(1))
                    diff = abs(v_time - s_time)

                    if diff < INITIAL_MATCH_THRESHOLD and diff < min_diff:
                        min_diff = diff
                        first_json = sub_file

        if not first_json:
            return []  # 返回空列表

        # 第二步: 链式查找后续 JSON
        matched_jsons = [first_json]
        current_json = first_json

        while True:
            current_mtime = current_json.stat().st_mtime
            next_json = None
            min_time_diff = 999999

            for sub_file in SUBTITLES_SOURCE_ROOT.rglob("*comments.json"):
                # 跳过已加入的
                if sub_file in matched_jsons:
                    continue
                
                sub_name = sub_file.stem

                # 必须同日期+同成员
                if v_date in sub_name and v_name in sub_name:
                    sub_ctime = sub_file.stat().st_ctime
                    time_diff = abs(sub_ctime - current_mtime)

                    # 确保时间顺序正确
                    if sub_ctime >= current_json.stat().st_ctime:
                        if time_diff < CONTINUATION_THRESHOLD and time_diff < min_time_diff:
                            min_time_diff = time_diff
                            next_json = sub_file

            if next_json:
                matched_jsons.append(next_json)
                logging.debug(f"找到后续 JSON: {next_json.name} (间隔 {min_time_diff:.0f}秒)")
                current_json = next_json
            else:
                break
            
        # 按创建时间排序
        matched_jsons.sort(key=lambda x: x.stat().st_ctime)

        logging.info(f"✅ 找到 {len(matched_jsons)} 个字幕文件")
        for idx, json_file in enumerate(matched_jsons, 1):
            logging.info(f"   {idx}. {json_file.name}")

        return matched_jsons  # 返回列表

    def move_subtitle_file(self, subtitle_files, video_id: str) -> bool:
        """移动字幕文件并重命名为视频ID"""
        processed_temp_file = None

        # 兼容单个文件或列表
        if isinstance(subtitle_files, Path):
            subtitle_files = [subtitle_files]

        try:
            # 固定使用 .ass 扩展名 (因为最终会转换成 ASS)
            target_filename = f"{video_id}.ass"
            target_path = SUBTITLES_TARGET_DIR / target_filename

            # 如果目标文件已存在，跳过
            if target_path.exists():
                logging.debug(f"字幕文件已存在，跳过: {target_filename}")
                return False

            # 调用 subtitle_processor 进行处理 (合并+转换+偏移)
            file_count = len(subtitle_files)
            first_file_name = subtitle_files[0].name if subtitle_files else "unknown"

            logging.debug(f"处理 {file_count} 个字幕文件: {first_file_name}, 应用 {SUBTITLE_OFFSET_SECONDS} 秒偏移...")

            processed_temp_file = subtitle_processor.offset_subtitle(
                source_path=subtitle_files, 
                offset_seconds=SUBTITLE_OFFSET_SECONDS, 
            )

            if not processed_temp_file:
                logging.error(f"字幕文件处理失败，停止发布")
                return False 

            # 复制文件（保留源文件）
            shutil.copy2(str(processed_temp_file), str(target_path))
            logging.debug(f"字幕文件已处理并复制: {first_file_name} -> {target_filename}")

            self.stats['moved_subtitles'] += 1
            return True

        except Exception as e:
            first_file_name = subtitle_files[0].name if subtitle_files else "unknown"
            error_msg = f"移动字幕文件失败 {first_file_name}: {e}"
            logging.error(error_msg)
            self.stats['errors'].append(error_msg)
            return False

        finally:
            # 清理临时文件
            if processed_temp_file and processed_temp_file.exists():
                try:
                    processed_temp_file.unlink()
                except Exception as e:
                    logging.error(f"清理临时字幕文件失败 {processed_temp_file.name}: {e}")

    def load_videos_json(self) -> Dict:
        """加载历史视频数据（优先从缓存读取，其次读取 jsonl）"""
        # 1. 检查缓存：如果已经加载过，直接返回内存中的数据
        if self._video_cache is not None:
            return self._video_cache

        # 2. 定义 jsonl 的路径
        jsonl_path = VIDEOS_JSON_PATH.with_suffix('.jsonl')
        videos = []
    
        # 3. 如果 jsonl 存在，逐行读取并解析
        if jsonl_path.exists():
            try:
                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            videos.append(json.loads(line))
                
                logging.debug(f"从 jsonl 加载了 {len(videos)} 条记录")
                # 将结果存入缓存
                self._video_cache = {"videos": videos}
                return self._video_cache
            except Exception as e:
                logging.error(f"读取 jsonl 失败: {e}")
    
        # 4. 如果 jsonl 不存在，再尝试读取旧版的 json
        if VIDEOS_JSON_PATH.exists():
            try:
                with open(VIDEOS_JSON_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 将结果存入缓存
                    self._video_cache = data
                    return data
            except Exception as e:
                logging.error(f"加载 json 失败: {e}")
    
        # 5. 如果都没有，初始化空缓存
        self._video_cache = {"videos": []}
        return self._video_cache
        
    def save_videos_json(self, data: Dict):
        """保存videos.json（JSON + JSONL双格式）"""
        self._video_cache = data
        try:
            # 保存JSONL格式（新版）
            jsonl_path = VIDEOS_JSON_PATH.with_suffix('.jsonl')
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                for video in data['videos']:
                    f.write(json.dumps(video, ensure_ascii=False) + '\n')
            logging.debug(f"videos.jsonl已保存（{len(data['videos'])}条）")

        except Exception as e:
            logging.error(f"保存文件失败: {e}")
    
    def add_video_to_json(self, video_info: Dict) -> bool:
        """添加视频信息到videos.json"""
        data = self.load_videos_json()
        
        # 检查是否已存在
        existing_ids = [v['id'] for v in data['videos']]
        if video_info['id'] in existing_ids:
            logging.warning(f"视频已存在于JSON中: {video_info['id']}")
            return False
        
        # 添加新视频
        data['videos'].insert(0, video_info)
        
        # 按日期排序（最新在前）
        data['videos'].sort(key=lambda x: x['date'], reverse=True)
        
        # 保存
        self.save_videos_json(data)
        self.stats['new_videos'] += 1
        logging.debug(f"新增视频: {video_info['id']} - {video_info.get('title', '')}")
        return True
    
    def process_video_file(self, video_file: Path) -> bool:
        """处理单个视频文件"""
        logging.info(f"处理视频: {video_file.name}")
        self.stats['processed_videos'] += 1
        
        # 获取视频ID
        video_id = self.get_video_id_from_uploaded_flag(video_file)
        if not video_id:
            return False
        
        # 查找字幕文件
        subtitle_files = self.find_subtitle_files(video_file)
        
        # 构建视频信息
        upload_date = self.extract_date_from_filename(video_file.stem)
        if not upload_date:
            upload_date = datetime.fromtimestamp(video_file.stat().st_mtime).strftime("%Y-%m-%d")
        
        video_info = {
            "id": video_id,
            "date": upload_date,
            "tags": ["录制", "视频"],
            "description": f"录制视频 - {video_file.stem}"
        }
        
        # 更新JSON
        json_updated = self.add_video_to_json(video_info)
        
        # 移动字幕文件
        subtitle_moved = False
        if subtitle_files:
            subtitle_moved = self.move_subtitle_file(subtitle_files, video_id)
        
        return json_updated or subtitle_moved
    
    def scan_uploaded_videos(self) -> List[Path]:
        """扫描已上传的视频"""
        uploaded_videos = []
        
        for uploaded_flag in MERGED_VIDEOS_DIR.glob("*.uploaded"):
            video_file = uploaded_flag.with_suffix('')
            if video_file.exists():
                uploaded_videos.append(video_file)
        
        logging.info(f"找到 {len(uploaded_videos)} 个已上传视频")
        return uploaded_videos
    
    def run_git_command(self, command: List[str]) -> Tuple[bool, str]:
        """执行Git命令"""
        try:
            logging.debug(f"执行: {' '.join(command)}")
            result = subprocess.run(
                command,
                cwd=str(GITHUB_PAGES_REPO_PATH),
                capture_output=True,
                text=True,
                timeout=GIT_TIMEOUT
            )
            
            if result.returncode == 0:
                return True, result.stdout
            else:
                return False, result.stderr
                
        except subprocess.TimeoutExpired:
            return False, "Git命令执行超时"
        except Exception as e:
            return False, str(e)
    
    def git_publish(self) -> bool:
        """执行Git发布"""
        if not ENABLE_GIT_AUTO_PUBLISH:
            logging.warning("Git自动发布已禁用")
            return True
        
        logging.info("开始Git发布...")
        
        try:
            # 1. 拉取最新代码
            if GIT_PULL_BEFORE_PUSH:
                success, output = self.run_git_command(['git', 'pull', 'origin', GIT_REMOTE_BRANCH])
                if not success:
                    logging.warning(f"Git pull失败: {output}")
                    return False
            
            # 2. 添加所有更改
            success, output = self.run_git_command(['git', 'add', '.'])
            if not success:
                logging.error(f"Git add失败: {output}")
                return False
            
            # 3. 检查是否有更改
            success, output = self.run_git_command(['git', 'status', '--porcelain'])
            if success and not output.strip():
                logging.info("没有需要提交的更改")
                return True
            
            # 4. 提交更改
            commit_message = GIT_COMMIT_MESSAGE_TEMPLATE.format(
                date=datetime.now().strftime("%Y-%m-%d %H:%M"),
                count=self.stats['new_videos']
            )
            
            success, output = self.run_git_command(['git', 'commit', '-m', commit_message])
            if not success:
                logging.error(f"Git commit失败: {output}")
                return False
            
            # 5. 推送到远程
            success, output = self.run_git_command(['git', 'push', 'origin', GIT_REMOTE_BRANCH])
            if not success:
                logging.error(f"Git push失败: {output}")
                return False
            
            logging.info("Git发布成功！")
            return True
            
        except Exception as e:
            logging.error(f"Git发布失败: {e}")
            return False
    
    def publish_all(self) -> bool:
        """发布所有已上传的视频"""
        logging.info("=== 开始GitHub Pages发布 ===")
        
        has_changes = self.process_recent_uploads()
        
        if not has_changes:
            uploaded_videos = self.scan_uploaded_videos()
        
            # 处理视频文件
            if uploaded_videos:
                for video_file in uploaded_videos:
                    try:
                        if self.process_video_file(video_file):
                            has_changes = True
                    except Exception as e:
                        error_msg = f"处理视频失败 {video_file.name}: {e}"
                        logging.error(error_msg)
                        self.stats['errors'].append(error_msg)

                        if not CONTINUE_ON_ERROR:
                            break
        
        # 如果有更改，执行Git发布
        if has_changes:
            git_success = self.git_publish()
        else:
            logging.info("没有需要发布的更改")
            git_success = True
        
        # 输出统计信息
        logging.info("=== 发布完成 ===")
        logging.debug(f"处理视频: {self.stats['processed_videos']}")
        logging.debug(f"新增视频: {self.stats['new_videos']}")
        logging.debug(f"移动字幕: {self.stats['moved_subtitles']}")
        
        if self.stats['errors']:
            logging.warning(f"错误数量: {len(self.stats['errors'])}")
            for error in self.stats['errors']:
                logging.error(f"  - {error}")
        
        return git_success and len(self.stats['errors']) == 0


    def load_recent_uploads(self) -> Dict:
        """加载最近的上传信息"""
        upload_info_file = MERGED_VIDEOS_DIR / "recent_uploads.json"

        if not upload_info_file.exists():
            return {"uploads": []}

        try:
            with open(upload_info_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"读取上传信息失败: {e}")
            return {"uploads": []}

    def process_recent_uploads(self) -> bool:
        """处理最近的上传信息"""
        upload_data = self.load_recent_uploads()
        has_changes = False

        for upload_info in upload_data["uploads"]:
            try:
                video_file = Path(upload_info["file_path"])
                video_id = upload_info["video_id"]
                self.stats['processed_videos'] += 1

                # 检查是否已经处理过
                if self.is_video_in_json(video_id):
                    continue
                
                # 查找字幕文件
                subtitle_files = self.find_subtitle_files(video_file)

                # 构建视频信息（使用上传时的实际信息）
                video_info = {
                    "id": video_id,
                    "date": upload_info["upload_time"][:10],  # 只取日期部分
                    "title": upload_info["title"],
                    # "description": upload_info["description"],
                    "tags": upload_info["tags"]
                }

                # 更新JSON
                json_updated = self.add_video_to_json(video_info)

                # 移动字幕文件
                subtitle_moved = False
                if subtitle_files:
                    subtitle_moved = self.move_subtitle_file(subtitle_files, video_id)

                if json_updated or subtitle_moved:
                    has_changes = True

            except Exception as e:
                error_msg = f"处理上传信息失败 {upload_info.get('filename', 'unknown')}: {e}"
                logging.error(error_msg)
                self.stats['errors'].append(error_msg)

        return has_changes

    def is_video_in_json(self, video_id: str) -> bool:
        """检查视频是否已在JSON中"""
        data = self.load_videos_json()
        existing_ids = [v['id'] for v in data['videos']]
        return video_id in existing_ids

# ==================== 公共接口函数 ====================

def publish_to_github_pages():
    """发布到GitHub Pages的公共接口"""
    try:
        publisher = GitHubPagesPublisher()
        success = publisher.publish_all()
        
        if PUBLISH_DELAY_SECONDS > 0:
            time.sleep(PUBLISH_DELAY_SECONDS)
        
        return success
        
    except Exception as e:
        logging.error(f"GitHub Pages发布失败: {e}")
        return False


def main():
    """主函数 - 独立运行接口"""
    success = publish_to_github_pages()
    if success:
        logging.info("发布任务完成！")
    else:
        logging.error("发布任务失败！")
        exit(1)


if __name__ == "__main__":
    main()
