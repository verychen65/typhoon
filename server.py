#!/usr/bin/env python3
"""台风数据静态文件服务器，数据通过 GitHub CDN 获取"""
import http.server
import urllib.request
import urllib.parse
import gzip
import io
import json
import os
import re
import threading
import time
from datetime import datetime, timezone, timedelta

PORT = 8090
DIRECTORY = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DIRECTORY, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# ============ 静态资源 gzip ============
# index.html 有 120KB，弱网下首屏要等很久；这几类文本资源压缩后只有原来的 1/5 左右。
GZIP_EXTS = {'.html', '.htm', '.css', '.js', '.json', '.svg', '.csv', '.txt'}
GZIP_MIN_BYTES = 1024        # 太小的文件压缩反而更慢，直接原样发
GZIP_LEVEL = 5
GZIP_CACHE_MAX = 24          # 压缩结果缓存条数（按 mtime+size 失效）
_gzip_cache = {}
_gzip_cache_lock = threading.Lock()


def get_gzipped(path, st):
    """按 mtime+size 缓存的 gzip 结果，避免每次请求都重新压缩。"""
    with _gzip_cache_lock:
        hit = _gzip_cache.get(path)
        if hit and hit[0] == st.st_mtime_ns and hit[1] == st.st_size:
            return hit[2]

    with open(path, 'rb') as fh:
        raw = fh.read()
    body = gzip.compress(raw, GZIP_LEVEL)

    with _gzip_cache_lock:
        if len(_gzip_cache) >= GZIP_CACHE_MAX:
            _gzip_cache.pop(next(iter(_gzip_cache)))
        _gzip_cache[path] = (st.st_mtime_ns, st.st_size, body)
    return body

# GitHub 镜像源配置（国内免梯子访问，数据由 Mac 自动化推送至 GitHub）
# 格式: https://cdn.jsdelivr.net/gh/{用户名}/{仓库名}@main/data/
GITHUB_MIRROR_BASE = os.environ.get('GITHUB_MIRROR_BASE', '')

# 镜像文件列表缓存 (避免频繁请求 GitHub API)
_mirror_files_cache = {'data': None, 'ts': 0.0}
MIRROR_CACHE_TTL = 600          # 列表有效期 10 分钟
MIRROR_COLD_TIMEOUT = 3         # 冷启动首次拉取，请求线程最多等 3 秒（硬上限）
MIRROR_BG_TIMEOUT = 25          # 后台刷新可以慢慢等（不阻塞任何请求）
MIRROR_RETRY_AFTER_FAIL = 120   # 拉取失败后 2 分钟再试
_mirror_refresh_lock = threading.Lock()
_mirror_refreshing = False


def _mirror_files_from_github(timeout):
    """真正去 GitHub 拉 data/ 目录文件列表。成功返回 set，失败返回 None。"""
    try:
        # 从 jsdelivr URL 提取 user/repo: https://cdn.jsdelivr.net/gh/USER/REPO@main/data/
        parts = GITHUB_MIRROR_BASE.split('/gh/')
        if len(parts) < 2:
            return None
        repo_part = parts[1].split('@')[0]  # USER/REPO
        api_url = f'https://api.github.com/repos/{repo_part}/contents/data'
        req = urllib.request.Request(api_url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/vnd.github.v3+json'
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            files = json.loads(resp.read().decode('utf-8'))
        return {f['name'] for f in files if f.get('name', '').endswith('.csv')}
    except Exception as e:
        print(f'  fetch_mirror_files error: {e}')
        return None


def _refresh_mirror_files(timeout=MIRROR_BG_TIMEOUT):
    """刷新镜像文件列表。成功替换，失败则保留旧列表并把重试时间推后。"""
    global _mirror_refreshing
    try:
        names = _mirror_files_from_github(timeout)
        now = time.time()
        if names is None:
            # 拉不到就保留上一次的结果：清空会让本来存在于镜像上的批次显示成"没有"
            if _mirror_files_cache['data'] is None:
                _mirror_files_cache['data'] = set()
            _mirror_files_cache['ts'] = now - MIRROR_CACHE_TTL + MIRROR_RETRY_AFTER_FAIL
        else:
            _mirror_files_cache['data'] = names
            _mirror_files_cache['ts'] = now
    finally:
        with _mirror_refresh_lock:
            _mirror_refreshing = False


def _start_mirror_refresh(timeout, name='mirror-refresh'):
    """没有在跑的刷新任务时启动一个后台线程；已在跑则不重复启动。"""
    global _mirror_refreshing
    with _mirror_refresh_lock:
        if _mirror_refreshing:
            return False
        _mirror_refreshing = True
    threading.Thread(target=_refresh_mirror_files, args=(timeout,), name=name, daemon=True).start()
    return True


def fetch_mirror_files():
    """返回镜像上已有的 CSV 文件名集合。

    关键点：绝不在请求线程里干等 GitHub。这条线路上 api.github.com 拉一次目录要
    十几秒才返回，原先每 10 分钟就有一个请求卡在这里，/api/latest 因此要等 10~20 秒。
    拉取一律放在后台线程里跑，请求线程最多只等 MIRROR_COLD_TIMEOUT 秒
    （注意 urllib 的 timeout 是"单次读写"超时，对慢速传输不构成总时长上限，
    所以这里用线程 + join 做真正的硬上限）。
    """
    if not GITHUB_MIRROR_BASE:
        return set()

    now = time.time()
    data = _mirror_files_cache['data']
    if data is not None and (now - _mirror_files_cache['ts']) < MIRROR_CACHE_TTL:
        return data

    cold = data is None
    started = _start_mirror_refresh(MIRROR_COLD_TIMEOUT if cold else MIRROR_BG_TIMEOUT)

    if cold and started:
        # 只有"进程启动后还没有任何结果"这一次会让请求线程稍等一下，
        # 保证 /api/latest_available 的首次判断不会漏掉只存在于镜像上的批次。
        deadline = time.time() + MIRROR_COLD_TIMEOUT
        while time.time() < deadline and _mirror_files_cache['data'] is None:
            time.sleep(0.05)
        data = _mirror_files_cache['data']

    return data or set()

class TyphoonHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def send_head(self):
        """静态文件支持 gzip。index.html 120KB → ~25KB，弱网下首屏快很多。

        注意：不能先调 super().send_head() 再补 Content-Encoding——
        父类在返回文件对象之前就已经 end_headers() 了，头改不动。
        """
        path = self.translate_path(self.path)

        # 目录 → 找 index.html；找不到就走父类（生成目录列表）
        if os.path.isdir(path):
            for name in ('index.html', 'index.htm'):
                candidate = os.path.join(path, name)
                if os.path.isfile(candidate):
                    path = candidate
                    break
            else:
                return super().send_head()

        ext = os.path.splitext(path)[1].lower()
        if ext not in GZIP_EXTS:
            return super().send_head()
        if 'gzip' not in self.headers.get('Accept-Encoding', '').lower():
            return super().send_head()

        try:
            st = os.stat(path)
        except OSError:
            return super().send_head()
        if st.st_size < GZIP_MIN_BYTES:
            return super().send_head()

        etag = f'W/"{st.st_size}-{int(st.st_mtime)}"'
        if self.headers.get('If-None-Match') == etag:
            self.send_response(304)
            self.end_headers()   # ETag / Cache-Control 由 end_headers 统一补
            return None

        body = get_gzipped(path, st)
        self.send_response(200)
        self.send_header('Content-Type', self.guess_type(path))
        self.send_header('Content-Encoding', 'gzip')
        self.send_header('Vary', 'Accept-Encoding')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Last-Modified', self.date_time_string(st.st_mtime))
        self.end_headers()
        return io.BytesIO(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == '/api/latest':
            self.handle_latest()
        elif parsed.path == '/api/latest_available':
            self.handle_latest_available()
        elif parsed.path == '/api/download':
            self.handle_download(parsed.query)
        elif parsed.path == '/api/cached':
            self.handle_cached()
        elif parsed.path == '/api/mirror_status':
            self.handle_mirror_status()
        elif parsed.path == '/api/clear_cache':
            self.handle_clear_cache(parsed.query)
        elif parsed.path == '/api/health':
            self.send_json({
                'status': 'ok',
                'mirror_base': GITHUB_MIRROR_BASE,
                'data_dir': DATA_DIR,
                'version': '2026-07-05'
            })
        elif parsed.path == '/api/typhoon-name':
            self.handle_typhoon_name(parsed.query)
        else:
            # 静态 CSV：ETag 匹配直接 304，浏览器复用缓存
            if parsed.path.endswith('.csv'):
                try:
                    p = self.translate_path(parsed.path)
                    st = os.stat(p)
                    etag = f'W/"{st.st_size}-{int(st.st_mtime)}"'
                    if self.headers.get('If-None-Match') == etag:
                        self.send_response(304)
                        self.end_headers()   # ETag / Cache-Control 由 end_headers 统一补
                        return
                except Exception:
                    pass
            super().do_GET()

    def handle_latest(self):
        """返回最近5天所有批次，标记可用性，按时间倒序"""
        now_utc = datetime.now(timezone.utc)
        mirror_files = fetch_mirror_files()
        batches = []
        # 过去5天到今天（共6天）的所有 00/06/12/18 UTC 批次
        for day_offset in range(-5, 1):
            base = now_utc + timedelta(days=day_offset)
            base = base.replace(hour=0, minute=0, second=0, microsecond=0)
            for hour in [0, 6, 12, 18]:
                t = base.replace(hour=hour)
                available = now_utc >= (t + timedelta(minutes=30))
                # Check if cached locally or on GitHub mirror
                cached = False
                mirror = False
                for model in ['OPER', 'WNV3']:
                    for dtype in ['ensemble', 'ensemble_mean']:
                        fn = f'{model}_{dtype}_{t.strftime("%Y_%m_%dT%H_00")}_paired.csv'
                        if os.path.exists(os.path.join(DATA_DIR, fn)):
                            cached = True
                        if fn in mirror_files:
                            mirror = True
                bj = (t + timedelta(hours=8)).strftime('%m-%d %H:00')
                utc_h = t.strftime('%H:%M')
                beijing_label = f'{bj}（{utc_h} UTC）'
                batches.append({
                    'init_time': t.strftime('%Y_%m_%dT%H_00'),
                    'utc_time': t.strftime('%Y-%m-%d %H:%M UTC'),
                    'beijing_time': beijing_label,
                    'timestamp': t.timestamp(),
                    'available': available,
                    'cached': cached,
                    'mirror': mirror
                })
        batches.sort(key=lambda x: x['timestamp'], reverse=True)
        self.send_json({'batches': batches, 'now_utc': now_utc.strftime('%Y-%m-%d %H:%M UTC'), 'mirror_enabled': bool(GITHUB_MIRROR_BASE)})

    def handle_latest_available(self):
        """返回镜像/本地有任一文件的最新批次（不要求完整4文件），附 complete 标记。
        放宽原因：DeepMind 分批发布（OPER 先、WNV3 后），最新批次常不完整；
        前端据此主动下载补齐，避免"首次加载永远停在次新完整批次"。
        """
        mirror_files = fetch_mirror_files()
        now_utc = datetime.now(timezone.utc)

        # 检查最近3天的所有批次，找到镜像/缓存中有文件的最新一个
        for day_offset in range(0, -4, -1):
            base = now_utc + timedelta(days=day_offset)
            base = base.replace(hour=0, minute=0, second=0, microsecond=0)
            for hour in [18, 12, 6, 0]:
                t = base.replace(hour=hour)
                init_time = t.strftime('%Y_%m_%dT%H_00')
                all_present = True
                any_present = False
                for model in ['OPER', 'WNV3']:
                    for dtype in ['ensemble', 'ensemble_mean']:
                        fn = f'{model}_{dtype}_{init_time}_paired.csv'
                        in_mirror = fn in mirror_files
                        in_local = os.path.exists(os.path.join(DATA_DIR, fn)) and os.path.getsize(os.path.join(DATA_DIR, fn)) > 1000
                        if in_mirror or in_local:
                            any_present = True
                        else:
                            all_present = False
                if any_present:
                    bj = (t + timedelta(hours=8)).strftime('%m-%d %H:00')
                    utc_h = t.strftime('%H:%M')
                    self.send_json({
                        'init_time': init_time,
                        'utc_time': t.strftime('%Y-%m-%d %H:%M UTC'),
                        'beijing_time': f'{bj}（{utc_h} UTC）',
                        'found': True,
                        'complete': all_present
                    })
                    return
        self.send_json({'found': False, 'message': '未找到可用的批次数据'})

    def handle_typhoon_name(self, query_str):
        """代理请求 agora.ex.nii.ac.jp 获取台风英文名
        track_id 格式: WP092026 → 对应 agora URL: 202609.ja.json
        """
        params = urllib.parse.parse_qs(query_str)
        track_id = params.get('track_id', [None])[0]
        if not track_id:
            self.send_json({'error': '缺少 track_id 参数'}, 400)
            return

        # 解析 track_id: 支持 WP092026 或 CP902026 格式
        # CP902026 → 编号=09 (第二位是padding), 年份=2026
        m = re.match(r'WP(\d{2})(\d{4})$', track_id)
        if m:
            storm_num = m.group(1)   # '09'
            year = m.group(2)       # '2026'
        else:
            m = re.match(r'CP(\d{2})(\d{4})$', track_id)
            if m:
                num_str = m.group(1)  # e.g. '90'
                year = m.group(2)    # '2026'
                num_val = int(num_str)
                # 如果编号以0结尾且数值大于30，视为单数编号+padding（如90→9）
                if num_str[1] == '0' and num_val > 30:
                    storm_num = num_str[0].zfill(2)  # '9' -> '09'
                else:
                    storm_num = num_str
            else:
                self.send_json({'error': 'track_id 格式错误，应为 WPXXYYYY 或 CPXXYYYY'}, 400)
                return
        url = f'https://agora.ex.nii.ac.jp/digital-typhoon/geojson/wnp/{year}{storm_num}.ja.json'

        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                name = data.get('properties', {}).get('name', '').strip()
                self.send_json({
                    'track_id': track_id,
                    'year': year,
                    'storm_num': storm_num,
                    'ename': name,
                    'found': bool(name)
                })
        except urllib.error.HTTPError as e:
            if e.code == 404:
                self.send_json({'track_id': track_id, 'found': False, 'ename': '', 'message': '该气旋数据尚未入库'})
            else:
                self.send_json({'error': f'HTTP {e.code}', 'found': False}, 502)
        except Exception as e:
            self.send_json({'error': str(e), 'found': False}, 500)

    def handle_cached(self):
        """返回本地已缓存的CSV文件列表"""
        cached = []
        if os.path.exists(DATA_DIR):
            for fn in sorted(os.listdir(DATA_DIR)):
                if fn.endswith('.csv') and os.path.getsize(os.path.join(DATA_DIR, fn)) > 1000:
                    # Parse: MODEL_TYPE_INITTIME_paired.csv where TYPE is ensemble or ensemble_mean
                    base = fn.replace('_paired.csv', '')
                    if base.startswith('OPER_ensemble_mean_'):
                        model = 'OPER'; dtype = 'ensemble_mean'
                        init_time = base[len('OPER_ensemble_mean_'):]
                    elif base.startswith('OPER_ensemble_'):
                        model = 'OPER'; dtype = 'ensemble'
                        init_time = base[len('OPER_ensemble_'):]
                    elif base.startswith('WNV3_ensemble_mean_'):
                        model = 'WNV3'; dtype = 'ensemble_mean'
                        init_time = base[len('WNV3_ensemble_mean_'):]
                    elif base.startswith('WNV3_ensemble_'):
                        model = 'WNV3'; dtype = 'ensemble'
                        init_time = base[len('WNV3_ensemble_'):]
                    else:
                        continue
                    cached.append({
                        'filename': fn,
                        'model': model,
                        'type': dtype,
                        'init_time': init_time,
                        'size': os.path.getsize(os.path.join(DATA_DIR, fn))
                    })
        # Group by init_time
        by_batch = {}
        for c in cached:
            by_batch.setdefault(c['init_time'], []).append(c)
        self.send_json({'cached_batches': sorted(by_batch.keys(), reverse=True), 'files': cached, 'total': len(cached)})

    def handle_mirror_status(self):
        """返回 GitHub 镜像源状态和文件列表"""
        files = fetch_mirror_files()
        self.send_json({
            'enabled': bool(GITHUB_MIRROR_BASE),
            'base_url': GITHUB_MIRROR_BASE,
            'file_count': len(files),
            'files': sorted(files)
        })

    def handle_clear_cache(self, query_str):
        """清除本地缓存CSV文件"""
        params = urllib.parse.parse_qs(query_str)
        target = params.get('target', ['all'])[0]  # all or batch:INIT_TIME
        deleted = []
        if os.path.exists(DATA_DIR):
            for fn in os.listdir(DATA_DIR):
                if not fn.endswith('.csv'):
                    continue
                if target == 'all':
                    pass
                elif target.startswith('batch:'):
                    batch = target[6:]
                    if batch not in fn:
                        continue
                else:
                    continue
                path = os.path.join(DATA_DIR, fn)
                try:
                    os.remove(path)
                    deleted.append(fn)
                except Exception as e:
                    print(f'  delete error {fn}: {e}')
        # 镜像列表标记为过期即可：保留旧数据，下一次访问在后台重新拉取，
        # 不要在这里把 data 置回 None（那会让下一个请求又去阻塞等 GitHub）
        _mirror_files_cache['ts'] = 0.0
        self.send_json({'deleted': deleted, 'count': len(deleted)})

    def handle_download(self, query_str):
        """代理下载 DeepMind CSV"""
        params = urllib.parse.parse_qs(query_str)
        init_time = params.get('init_time', [None])[0]
        data_type = params.get('type', ['ensemble'])[0]  # ensemble or ensemble_mean
        model = params.get('model', ['OPER'])[0]  # OPER or WNV3
        source = params.get('source', ['auto'])[0]  # auto, mirror

        if not init_time:
            self.send_json({'error': '缺少 init_time 参数'}, 400)
            return

        if data_type not in ('ensemble', 'ensemble_mean'):
            self.send_json({'error': '无效的 type 参数'}, 400)
            return

        if model not in ('OPER', 'WNV3'):
            self.send_json({'error': '无效的 model 参数'}, 400)
            return

        # 1. Check local cache first (skip if source=mirror)
        filename = f'{model}_{data_type}_{init_time}_paired.csv'
        cache_path = os.path.join(DATA_DIR, filename)
        if source == 'auto' and os.path.exists(cache_path) and os.path.getsize(cache_path) > 1000:
            st = os.stat(cache_path)
            etag = f'W/"{st.st_size}-{int(st.st_mtime)}"'
            if self.headers.get('If-None-Match') == etag:
                self.send_json(None, 304, etag=etag, cache_max_age=300)
                return
            with open(cache_path, 'r', encoding='utf-8') as f:
                csv_text = f.read()
            self.send_json({
                'csv': csv_text,
                'size': len(csv_text.encode('utf-8')),
                'url': 'local_cache',
                'filename': filename,
                'cached': True,
                'source': 'local_cache'
            }, etag=etag, cache_max_age=300)
            return

        # 2. Try GitHub mirror (jsdelivr CDN)
        if source in ('auto', 'mirror') and GITHUB_MIRROR_BASE:
            mirror_url = f'{GITHUB_MIRROR_BASE}/{filename}'
            try:
                req = urllib.request.Request(mirror_url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=8) as resp:
                    data = resp.read()
                    if len(data) > 1000 and not data.strip().startswith(b'<'):
                        csv_text = data.decode('utf-8')
                        with open(cache_path, 'w', encoding='utf-8') as f:
                            f.write(csv_text)
                        st = os.stat(cache_path)
                        etag = f'W/"{st.st_size}-{int(st.st_mtime)}"'
                        self.send_json({
                            'csv': csv_text,
                            'size': len(data),
                            'url': mirror_url,
                            'filename': filename,
                            'cached': False,
                            'source': 'github_mirror'
                        }, etag=etag, cache_max_age=300)
                        return
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    msg = f'该批次数据尚未同步到 GitHub，请稍后再试'
                    self.send_json({'error': msg, 'url': mirror_url}, 404)
                    return
                else:
                    print(f'  Mirror HTTP error for {filename}: {e.code}')
                    if source == 'mirror':
                        self.send_json({'error': f'GitHub镜像下载失败: HTTP {e.code}', 'url': mirror_url}, 502)
                        return
            except Exception as e:
                print(f'  Mirror miss for {filename}: {e}')
                if source == 'mirror':
                    self.send_json({'error': f'GitHub镜像下载失败: {e}', 'url': mirror_url}, 502)
                    return

        # 2b. Fallback to GitHub raw URL (if jsdelivr fails but source=auto)
        if source in ('auto', 'mirror') and GITHUB_MIRROR_BASE:
            try:
                parts = GITHUB_MIRROR_BASE.split('/gh/')
                if len(parts) >= 2:
                    repo_part = parts[1].split('@')[0]  # USER/REPO
                    raw_url = f'https://raw.githubusercontent.com/{repo_part}/main/data/{filename}'
                    req = urllib.request.Request(raw_url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        data = resp.read()
                        if len(data) > 1000 and not data.strip().startswith(b'<'):
                            csv_text = data.decode('utf-8')
                            with open(cache_path, 'w', encoding='utf-8') as f:
                                f.write(csv_text)
                            st = os.stat(cache_path)
                            etag = f'W/"{st.st_size}-{int(st.st_mtime)}"'
                            self.send_json({
                                'csv': csv_text,
                                'size': len(data),
                                'url': raw_url,
                                'filename': filename,
                                'cached': False,
                                'source': 'github_raw'
                            }, etag=etag, cache_max_age=300)
                            return
            except Exception as e:
                print(f'  GitHub raw fallback miss for {filename}: {e}')
                if source == 'mirror':
                    self.send_json({'error': f'GitHub镜像下载失败: {e}'}, 502)
                    return

        # 3. NAS 无梯子，不尝试 DeepMind 直连，数据由 Mac 自动化推送到 GitHub 后通过 CDN 获取
        self.send_json({'error': '该批次数据尚未同步到 GitHub，请等待自动化推送后再试'}, 404)

    def send_json(self, obj, code=200, etag=None, cache_max_age=0):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        # 条件请求：ETag 匹配直接 304，浏览器复用缓存（秒开）
        if etag and code in (200, 304) and self.headers.get('If-None-Match') == etag:
            self.send_response(304)
            self.send_header('ETag', etag)
            if cache_max_age > 0:
                self.send_header('Cache-Control', f'max-age={cache_max_age}')
            self.end_headers()
            return

        # /api/download 把整份 CSV 塞在 JSON 里（单文件 0.5~1.6MB），
        # 首屏要拉 4 份。这类内容压缩比很高，压完只剩 1/5 左右。
        gzipped = None
        if len(body) >= GZIP_MIN_BYTES and 'gzip' in self.headers.get('Accept-Encoding', '').lower():
            gzipped = gzip.compress(body, GZIP_LEVEL)

        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        if gzipped is not None:
            self.send_header('Content-Encoding', 'gzip')
            self.send_header('Vary', 'Accept-Encoding')
            self.send_header('Content-Length', str(len(gzipped)))
        else:
            self.send_header('Content-Length', str(len(body)))
        if etag and code == 200:
            self.send_header('ETag', etag)
            if cache_max_age > 0:
                self.send_header('Cache-Control', f'max-age={cache_max_age}')
        self.end_headers()
        self.wfile.write(gzipped if gzipped is not None else body)

    def end_headers(self):
        # 静态文件缓存策略：CSV 长缓存（max-age 内零请求，过期后 304 校验）；
        # HTML no-cache，每次带 ETag 回来校验，页面改完刷新即生效。
        # 按"实际文件"判断而不是 self.path：请求 "/" 时也要当成 index.html 处理，
        # 否则首页既不返回 ETag 也没有缓存指令，浏览器可能一直拿旧的缓存副本。
        cache_control, etag = self._static_cache_meta()
        if cache_control:
            self.send_header('Cache-Control', cache_control)
        if etag:
            self.send_header('ETag', etag)
        super().end_headers()

    def _static_cache_meta(self):
        """返回 (Cache-Control, ETag)；非静态 HTML/CSV 请求返回 (None, None)。"""
        try:
            path = self.translate_path(self.path)
            if os.path.isdir(path):
                path = os.path.join(path, 'index.html')
            ext = os.path.splitext(path)[1].lower()
            if ext in ('.html', '.htm'):
                cache_control = 'no-cache'
            elif ext == '.csv':
                cache_control = 'max-age=300'
            else:
                return None, None
            st = os.stat(path)
            return cache_control, f'W/"{st.st_size}-{int(st.st_mtime)}"'
        except Exception:
            return None, None

    def endheaders_with_cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()

if __name__ == '__main__':
    # 先把镜像文件列表在后台预热好，这样服务开始接请求时基本不会碰到冷启动等待
    if GITHUB_MIRROR_BASE:
        threading.Thread(target=fetch_mirror_files, name='mirror-warmup', daemon=True).start()

    # ThreadingHTTPServer：并发处理请求。
    # 原 HTTPServer 单线程，误差分析并发拉 54 文件时全部排队串行，首次加载极慢。
    server = http.server.ThreadingHTTPServer(('0.0.0.0', PORT), TyphoonHandler)
    print(f'服务器启动: http://localhost:{PORT}')
    print(f'API: http://localhost:{PORT}/api/latest  (获取最新批次)')
    print(f'API: http://localhost:{PORT}/api/download?model=OPER|WNV3&init_time=YYYY_MM_DDTHH_00&type=ensemble')
    if GITHUB_MIRROR_BASE:
        print(f'GitHub镜像源: {GITHUB_MIRROR_BASE} (国内免梯子)')
    else:
        print(f'GitHub镜像源: 未配置 (设置环境变量 GITHUB_MIRROR_BASE 启用)')
    server.serve_forever()
