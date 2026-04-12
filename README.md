# Showroom Autopilot

SHOWROOM ライブ配信の**全自動アーカイブシステム**。配信検出から録画・動画処理・YouTube アップロード・字幕生成・Web 公開まで、人手を一切介さずに完結するパイプラインを個人で設計・実装しました。

> **Note**: このプロジェクトはポートフォリオ目的で公開しています。個人学習・技術実証を目的として構築したものです。

---

## 技術スタック

| カテゴリ | 使用技術 |
|----------|----------|
| **言語** | Python 3.10+ |
| **非同期処理** | asyncio, httpx (AsyncClient) |
| **データベース** | Oracle Database (cx_Oracle, 接続プール, MERGE文) |
| **動画処理** | FFmpeg, FFprobe |
| **プロセス管理** | psutil, subprocess, fcntl (ファイルロック) |
| **外部 API** | YouTube Data API v3 (OAuth2, 再開可能アップロード) |
| **ファイル同期** | rsync over SSH |
| **インフラ** | Oracle Cloud (ARM インスタンス), systemd |
| **バージョン管理** | Git (GitHub Pages 自動デプロイ) |

---

## システム概要

### 解決した課題

SHOWROOM では配信のアーカイブ機能が提供されておらず、ライブ中にしか視聴できません。また、以下のような技術的困難が存在します。

- **ネットワーク断線**によって録画が複数フォルダに分割される
- HLS 方式のため**同一フラグメントが重複配信**されることがある
- 配信が不定期に始まるため**常時監視**が必要
- 複数メンバーを**同時並行**で扱う必要がある
- 配信コメントを字幕として映像に同期させる必要がある

これらをすべてソフトウェアで自動化しました。

### アーキテクチャ全体図

```
[Showroom API] ←── 非同期ポーリング (30 IP 分散, 5秒間隔)
       │
       ▼
[Oracle DB] ─── 配信状態をリアルタイム管理
       │
       ├─ IS_LIVE = 1 検出
       │       │
       │       ▼
       │  [ロードバランサー] ── 録画インスタンスを自動割当
       │       │
       │       ▼
       │  [録画プロセス管理] ── showroom.py を自動起動・監視
       │
       ▼
[TS ファイル検証] ── ffprobe で映像/音声ストリームを確認
       │
       ├─ 重複フラグメント排除 (MD5 + ファイルサイズ指紋)
       │
       ▼
[動画合併] ── ffmpeg concat (FileLock で多重実行防止)
       │
       ├─ 360p → 1080p アップスケール (別サーバーで並列処理)
       │
       ▼
[YouTube アップロード] ── 3 アカウントでクォータ分散
       │
       ├─ 字幕生成 (JSON コメントログ → ASS 弾幕字幕)
       │
       ▼
[GitHub Pages 自動公開] ── git commit & push
```

---

## 技術的な工夫・設計判断

### 1. 非同期 + IP 分散による大規模ポーリング

監視対象は 270 名以上のメンバー。単純な同期ループでは全員を 5 秒以内に確認することが不可能でした。

**解決策**: `asyncio` + `httpx.AsyncClient` を 30 個生成し、それぞれを異なる送信元 IP にバインド。`asyncio.Semaphore` でリクエスト数を IP 数以下に抑えつつ、メンバーごとに均等な時間間隔でリクエストを分散配置（スロットリング）。

```python
# 各ラウンド内で IP をシャッフルし、特定 IP への偏りを防止
shuffled_clients = ip_clients.copy()
random.shuffle(shuffled_clients)

# 時間的に均等分散させてリクエスト
interval = target_fill_time / total_members
await asyncio.sleep(index * interval)
```

**結果**: 270 名以上を 5 秒以内に並列チェック可能。IP あたりのレート制限を回避。

---

### 2. TS フラグメント重複排除（`TSDeduplicator`）

HLS 録画では、断線・再接続のたびに新しいフォルダが作成されますが、境界付近の数フラグメントが**両フォルダに重複して保存**されます。そのまま結合すると映像が一瞬繰り返されます。

**解決策**: ファイル全体ではなく**先頭 512KB の MD5 + ファイルサイズ**を指紋として使用。ファイル名やタイムスタンプではなくコンテンツで重複を判定。TTL 12 時間のシングルトン辞書でメンバーごとに管理し、クロスフォルダ重複をリアルタイム検出。

```python
hasher = hashlib.md5()
with open(ts_file, 'rb') as f:
    hasher.update(f.read(524288))  # 先頭 512KB のみ読む（高速化）
fingerprint = f"{hasher.hexdigest()}_{fsize}"
```

**理由**: TS ファイルは数 MB 以上あるため全体ハッシュはコストが高い。一方、先頭 512KB には映像の I フレームが含まれており、内容の同一性を十分に識別できる。

---

### 3. DB への高頻度書き込みの最適化

270 名 × 5 秒 = 毎分 3,000 件以上の状態更新が発生します。単純に 1 件ずつ INSERT/UPDATE すると DB への接続オーバーヘッドが問題になります。

**解決策**: バックグラウンドの専用スレッドがキューを 1 秒ごとにドレイン。同一メンバーの重複エントリを `dict` で排除した上で `executemany()` による**バッチ MERGE**。5 秒ごとにまとめてログ出力。

```python
# キュー内の全データを一括取得して dict で重複排除
batch_buffer = [data]
while not db_queue.empty():
    batch_buffer.append(db_queue.get_nowait())
unique_buffer = {d['member_id']: d for d in batch_buffer}  # 重複排除

# executemany でバッチ実行
cursor.executemany(merge_sql, all_bind_params)
```

**結果**: DB ラウンドトリップを大幅削減。接続断時はキューにデータを保持し、再接続後に自動フラッシュ。

---

### 4. 複数フォルダの同一配信統合

断線が複数回発生すると、1 回の配信が 3〜5 個のフォルダに分割されることがあります。

**解決策**: TS ファイルのタイムスタンプを比較し、**隣接フォルダ間の最後 TS と次フォルダの最初 TS の差が 5 分以内**であれば同一配信と判定してグループ化。フォルダ名や日付ではなく、実際のファイル更新時刻を根拠にした判定。

```python
time_gap = current_ts_time - prev_ts_time
if time_gap < 300:  # 5 分以内なら同一配信
    current_group.append(folder)
```

---

### 5. プロセス管理の「接管」と「自己起動」の分離

録画プロセス（`showroom.py`）は `showroom-smart-start.py` とは別プロセスで動作します。スクリプト再起動時に**すでに動いている録画プロセスを誤って停止・再起動してしまう**問題がありました。

**解決策**: psutil でスキャンしたプロセスを「接管プロセス（起動前から存在）」と「自己起動プロセス」に分類し、異なるポリシーを適用。

| 種別 | バッファ期間 | 異常時の挙動 |
|------|------------|-------------|
| 接管プロセス | 10 秒 | Kill のみ（再起動しない） |
| 自己起動プロセス | 35 秒 | 即時再起動 |

---

### 6. YouTube API クォータの多アカウント自動切替

YouTube Data API は 1 アカウントあたり 1 日 10,000 ユニット（アップロード 1 本 = 約 1,600 ユニット）の制限があります。

**解決策**: 3 アカウントを用意し、クォータ枯渇（`quotaExceeded`）を検出したタイミングで自動切替。リセット基準を太平洋時間 0:00 に統一し、日付文字列で枯渇状態を管理。

```python
LAST_QUOTA_EXHAUSTED_DATE = {
    'account1': None,
    'account2': None,
    'account3': None
}
# 枯渇検出時に当日の太平洋時間日付を記録 → 翌日まで自動スキップ
```

---

## 規模・実績

- **監視メンバー数**: 270 名以上（AKB48 / SKE48 / NMB48 / HKT48 / NGT48 / STU48）
- **処理フロー全長**: 配信検出 → YouTube 公開まで完全自動（人的操作ゼロ）
- **稼働環境**: Oracle Cloud 無料枠 ARM インスタンス（常時稼働）
- **アップロード実績**: 数百本以上のアーカイブ動画を自動公開

---

## ディレクトリ構成

```
showroom-autopilot/
├── monitor/         # 配信検出・DB 書き込み・ロードバランシング
├── recorder/        # 録画管理・TS 検証・動画合併・アップロード・字幕
└── shared/          # 設定・DB 接続・rsync 同期ユーティリティ
```

---

## セットアップ（概略）

```bash
# 依存パッケージ
pip install cx_Oracle httpx psutil google-api-python-client \
            google-auth-oauthlib PyYAML tabulate

# FFmpeg
sudo apt install ffmpeg

# DB 認証情報
echo "your_username" >  shared/db_credentials.key
echo "your_password" >> shared/db_credentials.key

# 監視起動
INSTANCE_ID=monitor-a python monitor/monitor_showroom.py

# 録画管理起動
INSTANCE_ID=recorder-a python recorder/showroom-smart-start.py
```

---

## 学んだこと・振り返り

- **非同期設計の重要性**: 同期処理では絶対に達成できなかったスループットを、asyncio と httpx の組み合わせで実現できた。
- **外部 API の制約との付き合い方**: YouTube のクォータ制限・Showroom のレート制限など、外部システムの制約をアーキテクチャレベルで設計に組み込む必要があると実感した。
- **冪等性の設計**: 何度実行しても同じ結果になるよう、MERGE 文・`.merged マーカー`・FileLock など、至る所で冪等性を意識した実装をした。
- **可観測性**: systemd ジャーナルへのログ出力・DB への状態書き込みにより、問題発生時の原因特定がしやすい設計を心がけた。
