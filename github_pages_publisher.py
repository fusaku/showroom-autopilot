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
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

# 导入配置
try:
    from config import *
except ImportError as e:
    log(f"无法导入配置文件: {e}")
    log("请确保config.py文件存在")
    raise
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
        log("配置验证失败:")
        for error in errors:
            log(f"  - {error}")
        return False
    
    return True

def ensure_directories():
    """确保必要的目录存在"""
    SUBTITLES_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    if VERBOSE_LOGGING:
        log(f"确保目录存在: {SUBTITLES_TARGET_DIR}")

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

    def log(self, message: str, level: str = "INFO"):
        """日志输出"""
        if VERBOSE_LOGGING or level in ["ERROR", "WARNING"]:
            timestamp = datetime.now().strftime('%m-%d %H:%M:%S')
            print(f"[{timestamp} - {__file__.split('/')[-1]}] {message}")
    
    def extract_date_from_filename(self, filename: str) -> Optional[str]:
        """从文件名提取日期 (例如: 250808 -> 2025-08-08)"""
        try:
            date_match = re.match(r'^(\d{6})', filename)
            if date_match:
                date_str = date_match.group(1)
                parsed_date = datetime.strptime(date_str, DATE_FORMAT_IN_FILENAME)
                return parsed_date.strftime("%Y-%m-%d")
        except Exception as e:
            self.log(f"从文件名提取日期失败 {filename}: {e}", "WARNING")
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
            
            self.log(f"无法从上传标记获取视频ID: {video_file.name}", "ERROR")
            return None
            
        except Exception as e:
            self.log(f"读取上传标记失败 {video_file.name}: {e}", "ERROR")
            return None
    
    def find_subtitle_file(self, video_file: Path) -> Optional[Path]:
        """查找对应的字幕文件"""
        video_filename = video_file.stem
        
        # 提取日期
        date_str = self.extract_date_from_filename(video_filename)
        if not date_str:
            return None
        
        # 构建字幕文件搜索路径
        # 路径格式: ~/Downloads/Showroom/2025-08-08/AKB48/comments/
        date_dir = SUBTITLES_SOURCE_ROOT / date_str
        
        if not date_dir.exists():
            self.log(f"日期目录不存在: {date_dir}", "WARNING")
            return None
        
        # 搜索comments目录下的字幕文件
        for subdir in date_dir.iterdir():
            if subdir.is_dir():
                comments_dir = subdir / "comments"
                if comments_dir.exists():
                    # 查找匹配的字幕文件
                    for ext in SUBTITLE_EXTENSIONS:
                        subtitle_file = comments_dir / f"{video_filename}{ext}"
                        # 带重试的文件存在检查
                        for attempt in range(3):  # 重试3次
                            if subtitle_file.exists():
                                self.log(f"找到字幕文件: {subtitle_file}")
                                return subtitle_file

                            if attempt < 2:  # 前两次失败时等待
                                time.sleep(3)  # 等待3秒
                                self.log(f"字幕文件检查重试 {attempt + 1}/3: {subtitle_file.name}")

        self.log(f"未找到字幕文件: {video_filename}", "WARNING")
        return None

    def move_subtitle_file(self, subtitle_file: Path, video_id: str) -> bool:
        """移动字幕文件并重命名为视频ID"""
        processed_temp_file = None
        try:
            target_filename = f"{video_id}{subtitle_file.suffix}"
            target_path = SUBTITLES_TARGET_DIR / target_filename
            
            # 如果目标文件已存在，跳过
            if target_path.exists():
                self.log(f"字幕文件已存在，跳过: {target_filename}")
                return False
            # === [新增逻辑开始] ===
            # 1. 调用 subtitle_processor 进行时间轴偏移，生成临时文件
            self.log(f"对字幕文件 {subtitle_file.name} 应用 {SUBTITLE_OFFSET_SECONDS} 秒的偏移...", "INFO")
            
            processed_temp_file = subtitle_processor.offset_subtitle(
                source_path=subtitle_file, 
                offset_seconds=SUBTITLE_OFFSET_SECONDS, 
                log_func=self.log
            )

            if not processed_temp_file:
                # 偏移处理失败，阻止发布
                self.log(f"字幕文件偏移处理失败，停止发布: {subtitle_file.name}", "ERROR")
                return False 
            # === [新增逻辑结束] ===
            
            # 复制文件（保留源文件）
            shutil.copy2(str(processed_temp_file), str(target_path))
            self.log(f"字幕文件已处理并复制: {subtitle_file.name} -> {target_filename}")
            
            self.stats['moved_subtitles'] += 1
            return True
            
        except Exception as e:
            error_msg = f"移动字幕文件失败 {subtitle_file.name}: {e}"
            self.log(error_msg, "ERROR")
            self.stats['errors'].append(error_msg)
            return False
    
        finally:
            # 3. 关键的清理步骤：无论成功或失败，都删除生成的临时文件
            if processed_temp_file and processed_temp_file.exists():
                try:
                    processed_temp_file.unlink()
                    self.log(f"清理临时文件: {processed_temp_file.name}")
                except Exception as e:
                    self.log(f"清理临时字幕文件失败 {processed_temp_file.name}: {e}", "ERROR")
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
                
                self.log(f"从 jsonl 加载了 {len(videos)} 条记录")
                # 将结果存入缓存
                self._video_cache = {"videos": videos}
                return self._video_cache
            except Exception as e:
                self.log(f"读取 jsonl 失败: {e}", "ERROR")
    
        # 4. 如果 jsonl 不存在，再尝试读取旧版的 json
        if VIDEOS_JSON_PATH.exists():
            try:
                with open(VIDEOS_JSON_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 将结果存入缓存
                    self._video_cache = data
                    return data
            except Exception as e:
                self.log(f"加载 json 失败: {e}", "ERROR")
    
        # 5. 如果都没有，初始化空缓存
        self._video_cache = {"videos": []}
        return self._video_cache
        
    def save_videos_json(self, data: Dict):
        """保存videos.json（JSON + JSONL双格式）"""
        self._video_cache = data
        try:
            # 保存传统JSON格式（兼容旧版）
            # with open(VIDEOS_JSON_PATH, 'w', encoding='utf-8') as f:
            #     json.dump(data, f, ensure_ascii=False, indent=2)
            # self.log("videos.json已保存")

            # 保存JSONL格式（新版）
            jsonl_path = VIDEOS_JSON_PATH.with_suffix('.jsonl')
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                for video in data['videos']:
                    f.write(json.dumps(video, ensure_ascii=False) + '\n')
            self.log(f"videos.jsonl已保存（{len(data['videos'])}条）")

        except Exception as e:
            self.log(f"保存文件失败: {e}", "ERROR")
    
    def add_video_to_json(self, video_info: Dict) -> bool:
        """添加视频信息到videos.json"""
        data = self.load_videos_json()
        
        # 检查是否已存在
        existing_ids = [v['id'] for v in data['videos']]
        if video_info['id'] in existing_ids:
            self.log(f"视频已存在于JSON中: {video_info['id']}")
            return False
        
        # 添加新视频
        data['videos'].insert(0, video_info)
        
        # 按日期排序（最新在前）
        data['videos'].sort(key=lambda x: x['date'], reverse=True)
        
        # 保存
        self.save_videos_json(data)
        self.stats['new_videos'] += 1
        self.log(f"新增视频: {video_info['id']} - {video_info.get('title', '')}")
        return True
    
    def process_video_file(self, video_file: Path) -> bool:
        """处理单个视频文件"""
        self.log(f"处理视频: {video_file.name}")
        self.stats['processed_videos'] += 1
        
        # 获取视频ID
        video_id = self.get_video_id_from_uploaded_flag(video_file)
        if not video_id:
            return False
        
        # 查找字幕文件
        subtitle_file = self.find_subtitle_file(video_file)
        
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
        if subtitle_file:
            subtitle_moved = self.move_subtitle_file(subtitle_file, video_id)
        
        return json_updated or subtitle_moved
    
    def scan_uploaded_videos(self) -> List[Path]:
        """扫描已上传的视频"""
        uploaded_videos = []
        
        for uploaded_flag in MERGED_VIDEOS_DIR.glob("*.uploaded"):
            video_file = uploaded_flag.with_suffix('')
            if video_file.exists():
                uploaded_videos.append(video_file)
        
        self.log(f"找到 {len(uploaded_videos)} 个已上传视频")
        return uploaded_videos
    
    def run_git_command(self, command: List[str]) -> Tuple[bool, str]:
        """执行Git命令"""
        try:
            self.log(f"执行: {' '.join(command)}")
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
            self.log("Git自动发布已禁用")
            return True
        
        self.log("开始Git发布...")
        
        try:
            # 1. 拉取最新代码
            if GIT_PULL_BEFORE_PUSH:
                success, output = self.run_git_command(['git', 'pull', 'origin', GIT_REMOTE_BRANCH])
                if not success:
                    self.log(f"Git pull失败: {output}", "WARNING")
                    return False
            
            # 2. 添加所有更改
            success, output = self.run_git_command(['git', 'add', '.'])
            if not success:
                self.log(f"Git add失败: {output}", "ERROR")
                return False
            
            # 3. 检查是否有更改
            success, output = self.run_git_command(['git', 'status', '--porcelain'])
            if success and not output.strip():
                self.log("没有需要提交的更改")
                return True
            
            # 4. 提交更改
            commit_message = GIT_COMMIT_MESSAGE_TEMPLATE.format(
                date=datetime.now().strftime("%Y-%m-%d %H:%M"),
                count=self.stats['new_videos']
            )
            
            success, output = self.run_git_command(['git', 'commit', '-m', commit_message])
            if not success:
                self.log(f"Git commit失败: {output}", "ERROR")
                return False
            
            # 5. 推送到远程
            success, output = self.run_git_command(['git', 'push', 'origin', GIT_REMOTE_BRANCH])
            if not success:
                self.log(f"Git push失败: {output}", "ERROR")
                return False
            
            self.log("Git发布成功！")
            return True
            
        except Exception as e:
            self.log(f"Git发布失败: {e}", "ERROR")
            return False
    
    def publish_all(self) -> bool:
        """发布所有已上传的视频"""
        self.log("=== 开始GitHub Pages发布 ===")
        
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
                        self.log(error_msg, "ERROR")
                        self.stats['errors'].append(error_msg)

                        if not CONTINUE_ON_ERROR:
                            break
        
        # 如果有更改，执行Git发布
        if has_changes:
            git_success = self.git_publish()
        else:
            self.log("没有需要发布的更改")
            git_success = True
        
        # 输出统计信息
        self.log("=== 发布完成 ===")
        self.log(f"处理视频: {self.stats['processed_videos']}")
        self.log(f"新增视频: {self.stats['new_videos']}")
        self.log(f"移动字幕: {self.stats['moved_subtitles']}")
        
        if self.stats['errors']:
            self.log(f"错误数量: {len(self.stats['errors'])}", "WARNING")
            for error in self.stats['errors']:
                self.log(f"  - {error}", "ERROR")
        
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
            self.log(f"读取上传信息失败: {e}", "ERROR")
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
                subtitle_file = self.find_subtitle_file(video_file)

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
                if subtitle_file:
                    subtitle_moved = self.move_subtitle_file(subtitle_file, video_id)

                if json_updated or subtitle_moved:
                    has_changes = True

            except Exception as e:
                error_msg = f"处理上传信息失败 {upload_info.get('filename', 'unknown')}: {e}"
                self.log(error_msg, "ERROR")
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
        log(f"GitHub Pages发布失败: {e}")
        return False


def main():
    """主函数 - 独立运行接口"""
    success = publish_to_github_pages()
    if success:
        log("发布任务完成！")
    else:
        log("发布任务失败！")
        exit(1)


if __name__ == "__main__":
    main()
