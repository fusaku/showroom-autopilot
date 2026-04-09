# Showroom Autopilot

SHOWROOM 配信の全自動監視・録画・処理・アップロードシステム。AKB48を中心とした48Gグループメンバーの配信アーカイブに特化して設計されています。

## 目次

- [機能概要](#機能概要)
- [システム構成](#システム構成)
- [ディレクトリ構造](#ディレクトリ構造)
- [環境要件](#環境要件)
- [セットアップ](#セットアップ)
- [起動方法](#起動方法)
- [設定詳細](#設定詳細)
- [処理フロー](#処理フロー)
- [アーキテクチャメモ](#アーキテクチャメモ)

---

## 機能概要

### 実装済み機能

| カテゴリ | 機能 |
|----------|------|
| **監視** | 非同期HTTPによるリアルタイム配信状態ポーリング（30個のIPアドレスを使ったIP分散） |
| **録画管理** | psutil によるプロセス管理・重複プロセス自動排除・録画プロセスの自動再起動 |
| **ファイル検査** | ffprobe による TS フラグメントの映像/音声ストリーム検証 |
| **重複排除** | MD5 ハッシュ + ファイルサイズによるクロスフォルダ重複 TS 検出 (`TSDeduplicator`) |
| **マルチフォルダ統合** | 接続断による複数フォルダ分割を自動検出・同一配信として統合 |
| **字幕処理** | JSON コメントログ → ASS 字幕変換、時間軸オフセット調整、マルチ JSON 結合 |
| **動画合併** | ffmpeg concat によるフラグメント統合（ファイルロック付きで多重処理防止） |
| **YouTube アップロード** | OAuth2 認証、複数アカウントによる API クォータ分散、再開可能アップロード |
| **GitHub Pages 発布** | `videos.jsonl` + 字幕ファイルの自動 Git コミット＆プッシュ |
| **4C 連携** | rsync による別サーバーへのリアルタイム TS 同期（分解能判定付き） |
| **アップスケール** | 360p 配信を 1080p へ ffmpeg 変換（4C サーバー上で実行） |
| **Oracle Bucket** | Oracle Object Storage への完成 MP4 自動アップロード |
| **クォータ管理** | 太平洋時間 0:00 基準の YouTube API クォータ自動切替 |
| **ロードバランシング** | 複数録画インスタンスへの配信タスク自動割当 |
| **負荷分散監視** | 複数検出インスタンスによるメンバーのシャード分割監視 |

---

## システム構成

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Oracle Database (SRDB)                        │
│         LIVE_STATUS / INSTANCES / MEMBER_INSTANCES / MEMBERS         │
│                   SHOWROOM_LIVE_HISTORY / GROUPS                     │
└────────┬────────────────────────────┬───────────────────────────────┘
         │                            │
┌────────┴────────┐          ┌────────┴──────────────────────────┐
│  Monitor モジュール  │          │         Recorder モジュール          │
│  (3C サーバー)   │          │         (3C サーバー)               │
├─────────────────┤          ├──────────────────────────────────┤
│ monitor_showroom│          │ showroom-smart-start.py          │
│  .py            │          │  ∟ psutil でプロセス管理           │
│  ∟ 30 IP 分散   │          │  ∟ DB から担当メンバー取得          │
│  ∟ httpx 非同期  │          │                                  │
│  ∟ DB 書込スレッド│          │ checker.py                       │
│  ∟ ロードバランサ  │          │  ∟ TS 検証 (ffprobe)             │
│                 │          │  ∟ TSDeduplicator                │
│ manage_instances│          │  ∟ 字幕待機ロジック                 │
│  .py            │          │  ∟ merge_queue → merger.py       │
└─────────────────┘          │  ∟ rsync → 4C サーバー            │
                              │                                  │
                              │ merger.py                        │
                              │  ∟ ffmpeg concat                 │
                              │  ∟ FileLock 排他制御              │
                              │  ∟ upload_youtube.py 呼出し      │
                              │                                  │
                              │ upload_youtube.py                │
                              │  ∟ Google API OAuth2             │
                              │  ∟ 3 アカウント クォータ管理        │
                              │  ∟ github_pages_publisher.py     │
                              │  ∟ Oracle Bucket アップロード     │
                              └──────────────────────────────────┘
                                           ↓ rsync (360p TSのみ)
                              ┌──────────────────────────────────┐
                              │      4C サーバー (checker_4c.py)  │
                              │  ∟ upscaler.py (360p → 1080p)   │
                              │  ∟ merger.py (1080p 版合併)      │
                              └──────────────────────────────────┘
```

### コンポーネント説明

**Monitor（検出器）**
- `monitor_showroom.py`: 非同期 httpx クライアントを最大 30 個生成し、各クライアントを異なる送信元 IP にバインドして Showroom API を並列ポーリング。状態変化をキューに積み、バックグラウンドスレッドが Oracle DB へバッチ書き込み。
- `load_balancer_module.py`: 配信開始イベント発生時に `MEMBER_INSTANCES` テーブルへ自動的に録画インスタンスを割り当てる。
- `manage_instances.py`: インスタンス一覧・登録・状態変更・ハートビート更新 の CLI ツール。

**Recorder（録画管理）**
- `showroom-smart-start.py`: DB から本インスタンス担当メンバーを取得し、psutil で全 `showroom.py` プロセスをスキャン。重複プロセスは最古のものを保留して残りをkill。接管プロセスには 10 秒バッファ期間を設ける。
- `checker.py`: 配信フォルダを監視し、TS ファイルを逐次 ffprobe 検証。`TSDeduplicator` で断線再接続による重複フラグメントをMD5で排除。配信終了・字幕確認後に `merge_queue` へタスクを投入。
- `checker_4c.py`: 4C サーバー上で動作。rsync 経由で届いた 360p TS を ffmpeg で 1080p にアップスケールし、完了後に merger へ渡す。
- `merger.py`: `filelist.txt` を元に ffmpeg concat で MP4 を生成。FileLock で多重実行を防止。完了後に `upload_youtube.py` を非同期サブプロセスとして起動。
- `restart_handler.py`: TS ファイルの更新停止を監視して `showroom-*.service` を自動再起動。MD5ハッシュによる"ゴースト録画"（HLS デッドループ）検出も実装。
- `upload_youtube.py`: 3 アカウント（主推専用・AKB48汎用・その他）のクォータを太平洋時間で管理。再開可能アップロードに 30 秒タイムアウトを設定し、失敗時はアップロードセッションを再作成して最大 5 回リトライ。
- `github_pages_publisher.py`: アップロード後に `videos.jsonl` と処理済み ASS 字幕ファイルを GitHub Pages リポジトリへコミット・プッシュ。
- `subtitle_processor.py`: JSON コメントログを弾幕スタイル ASS 字幕に変換。複数 JSON の自動結合・破損 JSON の修復・時間軸オフセット適用に対応。

**Shared（共有モジュール）**
- `config.py`: 全設定の一元管理。DB 接続・パス・YouTube API・Git・Oracle Bucket・rsync 同期モードを定義。
- `db_members_loader.py`: Oracle DB からメンバー設定をロード（60 秒キャッシュ付き）。YAML ファイルを廃止して DB を単一ソースとする。
- `sync_module.py`: rsync による 4C サーバーへの TS 同期。ffprobe で分解能を確認し、720p 以上は同期スキップ（4C は低解像度のみ担当）。`should_run_local_upload()` でYouTubeアップロードの担当サーバーを判定。
- `logger_config.py`: systemd ジャーナル向けのシンプルなログ初期化。

---

## ディレクトリ構造

```
showroom-autopilot/
├── data/                          # メンバー設定 YAML (DB の初期値/参照用)
│   ├── AKB48_members.yaml
│   ├── SKE48_members.yaml
│   ├── NMB48_members.yaml
│   ├── HKT48_members.yaml
│   ├── NGT48_members.yaml
│   └── STU48_members.yaml
│
├── monitor/                       # 配信検出モジュール
│   ├── monitor_showroom.py        # 非同期メイン検出ループ
│   ├── load_balancer_module.py    # 録画インスタンス割当
│   ├── manage_instances.py        # インスタンス管理 CLI
│   ├── config.py → shared/config.py
│   ├── logger_config.py → shared/logger_config.py
│   └── db_members_loader.py → shared/db_members_loader.py
│
├── recorder/                      # 録画・処理モジュール
│   ├── showroom-smart-start.py    # メイン録画管理デーモン
│   ├── checker.py                 # TS 検証・合併トリガー (3C)
│   ├── checker_4c.py              # TS アップスケール・合併 (4C)
│   ├── merger.py                  # ffmpeg 動画合併
│   ├── subtitle_processor.py      # JSON → ASS 字幕変換
│   ├── upload_youtube.py          # YouTube アップロード
│   ├── github_pages_publisher.py  # GitHub Pages 公開
│   ├── restart_handler.py         # 録画プロセス監視・再起動
│   ├── upscaler.py                # 360p → 1080p 変換 (4C)
│   ├── cleanup.py                 # 完了済みファイル削除
│   ├── upload_oracle_bucket_wallet.py  # Oracle Bucket アップロード
│   ├── config.py → shared/config.py
│   ├── logger_config.py → shared/logger_config.py
│   └── db_members_loader.py → shared/db_members_loader.py
│
└── shared/                        # 全モジュール共通
    ├── config.py                  # 全設定定義
    ├── db_members_loader.py       # DB メンバーローダー
    ├── logger_config.py           # ログ設定
    ├── sync_module.py             # rsync 同期・分流判定
    ├── db_credentials.key         # DB 認証情報 (要設定)
    ├── bucket_credentials.key     # Oracle Bucket 設定 (任意)
    ├── 4c24g_server.conf          # 4C サーバー接続情報 (任意)
    └── credentials/               # YouTube OAuth クレデンシャル
        ├── autoupsr/              # 主推専用アカウント
        ├── 48g-SR/                # AKB48 汎用アカウント
        └── idol-SR/               # その他グループ用アカウント
```

---

## 環境要件

### システム

- **OS**: Ubuntu 24.04 LTS
- **Python**: 3.10 以上
- **ストレージ**: 100GB 以上（録画一時保存用）

### 必須ソフトウェア

```bash
# FFmpeg / FFprobe
sudo apt install ffmpeg

# Python パッケージ
pip install cx_Oracle httpx psutil PyYAML google-api-python-client \
            google-auth-oauthlib tabulate

# Showroom Live Watcher (録画ツール)
# https://github.com/wlerin/showroom
```

### 必須インフラ

| 項目 | 詳細 |
|------|------|
| Oracle Database | Wallet ファイル + TNS 設定 |
| Oracle Cloud VM | 無料枠 ARM インスタンス推奨 |
| YouTube Data API v3 | Google Cloud Console で有効化 |

---

## セットアップ

### 1. DB 認証情報の設定

```
shared/db_credentials.key
```
```
your_oracle_username
your_oracle_password
```

### 2. Oracle Wallet の配置

```bash
# Wallet を展開
unzip Wallet_SRDB.zip -d /home/ubuntu/Wallet_SRDB

# config.py の WALLET_DIR を確認
WALLET_DIR = "/home/ubuntu/Wallet_SRDB"
TNS_ALIAS  = "srdb_high"
```

### 3. DB テーブルの作成

最低限必要なテーブル:

| テーブル名 | 用途 |
|-----------|------|
| `ADMIN.LIVE_STATUS` | 配信リアルタイム状態 |
| `ADMIN.MEMBERS` | メンバーマスター |
| `ADMIN.GROUPS` | グループマスター |
| `ADMIN.YOUTUBE_CONFIGS` | YouTube 設定 |
| `ADMIN.YOUTUBE_TAGS` | タグ |
| `ADMIN.INSTANCES` | 監視・録画インスタンス |
| `ADMIN.MEMBER_INSTANCES` | インスタンス割当 |
| `ADMIN.SHOWROOM_LIVE_HISTORY` | 配信履歴 |

### 4. インスタンス登録

```bash
# 監視インスタンス登録
python monitor/manage_instances.py --register monitor-a monitor "検出器A" --capacity 300

# 録画インスタンス登録
python monitor/manage_instances.py --register recorder-a recorder "録画サーバーA" --capacity 20
```

### 5. YouTube 認証

```bash
# 初回のみブラウザ認証が必要
python recorder/upload_youtube.py
```
OAuth 認証完了後、`shared/credentials/*/youtube_token.pickle` に保存されます。

### 6. 4C サーバー設定（任意）

360p 配信を 1080p へアップスケールして保存する場合:

```
shared/4c24g_server.conf
```
```
192.168.x.x          # リモートサーバー IP
22                   # SSH ポート
```

---

## 起動方法

### 監視サービス（3C サーバー）

```bash
# 単一インスタンス（全メンバー監視）
INSTANCE_ID=monitor-a python monitor/monitor_showroom.py

# 複数インスタンス（DB のアクティブインスタンス数で自動シャード分割）
# サーバー A
INSTANCE_ID=monitor-a python monitor/monitor_showroom.py
# サーバー B
INSTANCE_ID=monitor-b python monitor/monitor_showroom.py
```

### 録画管理サービス（3C サーバー）

```bash
INSTANCE_ID=recorder-a python recorder/showroom-smart-start.py
```

### TS 検証・合併サービス（3C サーバー）

```bash
python recorder/checker.py
```

### 4C サーバー

```bash
python recorder/checker_4c.py
```

### インスタンス状態確認

```bash
python monitor/manage_instances.py --list
python monitor/manage_instances.py --load
```

---

## 設定詳細

主要な設定は `shared/config.py` に集約されています。

### パス設定

```python
PARENT_DIR  = Path("~/Downloads/Showroom/active").expanduser()  # 録画フォルダ
OUTPUT_DIR  = Path("/mnt/video/merged").expanduser()            # 完成 MP4 出力先
LOCK_DIR    = OUTPUT_DIR / ".locks"                             # ファイルロック置き場
```

### 監視タイミング

```python
REQUEST_INTERVAL        = 5    # Showroom API ポーリング間隔（秒）
CHECK_INTERVAL          = 30   # ファイル監視ループ間隔（秒）
LIVE_INACTIVE_THRESHOLD = 60   # 配信終了判定（秒）
FINAL_INACTIVE_THRESHOLD= 60   # 最終終了確認（秒）
FILE_CHECK_GRACE_PERIOD = 35   # 録画プロセス起動後の猶予時間（秒）
STOP_DELAY              = 300  # 配信終了後に録画プロセスを停止するまでの待機（秒）
```

### TS 重複排除

```python
# TSDeduplicator の TTL
ttl = 43200  # 12 時間（同一配信の断線再接続をカバー）
# MD5 計算に使用するバイト数
hasher.update(f.read(524288))  # 先頭 512KB
```

### rsync 同期モード（`SYNC_MODE`）

| 値 | 動作 |
|----|------|
| `"main"` | `MAIN_MEMBER_ID` で指定したメンバーの 360p TS のみ 4C へ転送 |
| `"all"` | 全メンバーの 360p TS を転送 |
| `"off"` | 同期無効（3C でのみ処理） |

### YouTube アカウント分岐

| 対象 | アカウント |
|------|-----------|
| 橋本陽菜 (`Hashimoto Haruna`) | 主アカウント (`autoupsr`) |
| AKB48 メンバー | 副アカウント (`48g-SR`) |
| その他グループ | 第三アカウント (`idol-SR`)（現在無効） |

クォータ管理は太平洋時間 0:00 を基準に日次リセット。枯渇時は自動で別アカウントへ切替。

### 字幕時間軸オフセット

```python
SUBTITLE_OFFSET_SECONDS = 12  # 録画開始から配信映像までの遅延補正（秒）
```

---

## 処理フロー

```
Showroom API ポーリング（30 IP 並列）
        │
        ▼
IS_LIVE = 1 を検出
        │
        ├─ LoadBalancer.assign_recorder()
        │      ∟ MEMBER_INSTANCES に録画インスタンスを登録
        │
        ▼
showroom-smart-start.py が検出
        │
        ▼
showroom.py 録画プロセス起動
        │ TS フラグメントを連続生成
        ▼
checker.py が監視
        │
        ├─ TSDeduplicator で重複排除
        ├─ ffprobe で映像/音声ストリーム検証
        ├─ 有効 TS を valid_files に追加
        ├─ rsync で 4C サーバーに同期（360p のみ）
        │
        ▼ IS_LIVE = 0 かつ TS 更新停止
        │
        ├─ 字幕 JSON の存在確認（最大 5 回待機）
        ├─ finalize_live_check() → filelist.txt 生成
        │
        ▼
merge_queue に投入 → merger.py
        │
        ├─ ffmpeg concat → .mp4 生成
        ├─ .merged マーカー作成
        │
        ▼
upload_youtube.py（別プロセスとして非同期起動）
        │
        ├─ アカウント判定・OAuth2 認証
        ├─ 再開可能アップロード（128MB チャンク）
        ├─ プレイリストに追加
        ├─ .uploaded マーカーに video_id を記録
        ├─ Oracle Bucket にもアップロード
        │
        ▼
github_pages_publisher.py
        │
        ├─ 字幕 JSON → ASS 変換・時間軸オフセット適用
        ├─ videos.jsonl に追記
        └─ git add / commit / push
```

---

## アーキテクチャメモ

### なぜ YAML でなく DB か

`db_members_loader.py` が Oracle DB から直接メンバー設定を取得します（60 秒キャッシュ）。`data/` 以下の YAML ファイルは DB 初期投入の参照用として残っており、ランタイムでは使用されません。

### フォルダ分割の統合ロジック

SHOWROOM の HLS 録画では、ネットワーク断線が発生するたびに新しいフォルダが生成されます。`checker.py` の `group_folders_by_member()` は、同一メンバーのフォルダ群を TS ファイルのタイムスタンプ差（5 分以内）で判定し、同一配信として統合してから merger に渡します。

### 4C サーバーへの分流

`sync_module.py` の `should_run_local_upload()` が 3C/4C の役割分担を決定します。ffprobe で TS の分解能を確認し、360p の場合は 4C サーバーへ rsync 転送。4C 側で 1080p にアップスケールしてから合併・配布します。720p 以上の高解像度配信は 3C がそのまま処理します。

### プロセス管理の設計

`showroom-smart-start.py` はループごとに 1 回だけ `psutil.process_iter()` を呼び出し、全 showroom.py プロセスをスキャンします。各プロセスを「接管プロセス（起動前から存在）」と「自己起動プロセス」に分類し、それぞれ異なるバッファ時間と再起動ポリシーを適用することで、誤再起動を防ぎます。

### ファイルロック

merger.py は `fcntl.LOCK_EX | LOCK_NB` によるノンブロッキングの排他ロックを使用し、同一ファイルへの多重 ffmpeg 実行を防止します。checker.py の merge_queue はシングルスレッドのワーカーがシリアルに処理するため、キュー側での競合は発生しません。
