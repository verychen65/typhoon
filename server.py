#!/usr/bin/env python3
"""台风数据静态文件服务器，数据通过 GitHub CDN 获取"""
import http.server
import urllib.request
import urllib.parse
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta

PORT = 8090
DIRECTORY = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DIRECTORY, 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# GitHub 镜像源配置（国内免梯子访问，数据由 Mac 自动化推送至 GitHub）
# 格式: https://cdn.jsdelivr.net/gh/{用户名}/{仓库名}@main/data/
GITHUB_MIRROR_BASE = os.environ.get('GITHUB_MIRROR_BASE', '')

# 镜像文件列表缓存 (避免频繁请求 GitHub API)
_mirror_files_cache = {'data': None, 'ts': 0}
MIRROR_CACHE_TTL = 600  # 10分钟

def fetch_mirror_files():
    """获取 GitHub 仓库 data/ 目录下的文件列表，缓存10分钟"""
    if not GITHUB_MIRROR_BASE:
        return set()
    now = time.time()
    if _mirror_files_cache['data'] is not None and (now - _mirror_files_cache['ts']) < MIRROR_CACHE_TTL:
        return _mirror_files_cache['data']
    # 从 jsdelivr URL 提取 user/repo: https://cdn.jsdelivr.net/gh/USER/REPO@main/data/
    try:
        parts = GITHUB_MIRROR_BASE.split('/gh/')
        if len(parts) < 2:
            return set()
        repo_part = parts[1].split('@')[0]  # USER/REPO
        api_url = f'https://api.github.com/repos/{repo_part}/contents/data'
        req = urllib.request.Request(api_url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/vnd.github.v3+json'
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            files = json.loads(resp.read().decode('utf-8'))
            names = {f['name'] for f in files if f.get('name', '').endswith('.csv')}
            _mirror_files_cache['data'] = names
            _mirror_files_cache['ts'] = now
            return names
    except Exception as e:
        print(f'  fetch_mirror_files error: {e}')
        return set()

class TyphoonHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

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
                for model in ['FNV3', 'GENC']:
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
        """返回实际可下载的最新批次（GitHub镜像或本地缓存中有完整4文件的最新批次）"""
        mirror_files = fetch_mirror_files()
        now_utc = datetime.now(timezone.utc)

        # 检查最近3天的所有批次，找到镜像/缓存中完整存在的最新一个
        for day_offset in range(0, -4, -1):
            base = now_utc + timedelta(days=day_offset)
            base = base.replace(hour=0, minute=0, second=0, microsecond=0)
            for hour in [18, 12, 6, 0]:
                t = base.replace(hour=hour)
                init_time = t.strftime('%Y_%m_%dT%H_00')
                # 检查4个文件是否都在镜像或本地缓存中
                all_present = True
                sources = []
                for model in ['FNV3', 'GENC']:
                    for dtype in ['ensemble', 'ensemble_mean']:
                        fn = f'{model}_{dtype}_{init_time}_paired.csv'
                        in_mirror = fn in mirror_files
                        in_local = os.path.exists(os.path.join(DATA_DIR, fn)) and os.path.getsize(os.path.join(DATA_DIR, fn)) > 1000
                        if not (in_mirror or in_local):
                            all_present = False
                            break
                        sources.append('mirror' if in_mirror else 'local')
                    if not all_present:
                        break
                if all_present:
                    bj = (t + timedelta(hours=8)).strftime('%m-%d %H:00')
                    utc_h = t.strftime('%H:%M')
                    self.send_json({
                        'init_time': init_time,
                        'utc_time': t.strftime('%Y-%m-%d %H:%M UTC'),
                        'beijing_time': f'{bj}（{utc_h} UTC）',
                        'found': True
                    })
                    return
        self.send_json({'found': False, 'message': '未找到完整的批次数据'})

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
                    if base.startswith('FNV3_ensemble_mean_'):
                        model = 'FNV3'; dtype = 'ensemble_mean'
                        init_time = base[len('FNV3_ensemble_mean_'):]
                    elif base.startswith('FNV3_ensemble_'):
                        model = 'FNV3'; dtype = 'ensemble'
                        init_time = base[len('FNV3_ensemble_'):]
                    elif base.startswith('GENC_ensemble_mean_'):
                        model = 'GENC'; dtype = 'ensemble_mean'
                        init_time = base[len('GENC_ensemble_mean_'):]
                    elif base.startswith('GENC_ensemble_'):
                        model = 'GENC'; dtype = 'ensemble'
                        init_time = base[len('GENC_ensemble_'):]
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
        # Also clear mirror cache
        global _mirror_files_cache
        _mirror_files_cache = {'data': None, 'ts': 0}
        self.send_json({'deleted': deleted, 'count': len(deleted)})

    def handle_download(self, query_str):
        """代理下载 DeepMind CSV"""
        params = urllib.parse.parse_qs(query_str)
        init_time = params.get('init_time', [None])[0]
        data_type = params.get('type', ['ensemble'])[0]  # ensemble or ensemble_mean
        model = params.get('model', ['FNV3'])[0]  # FNV3 or GENC
        source = params.get('source', ['auto'])[0]  # auto, mirror

        if not init_time:
            self.send_json({'error': '缺少 init_time 参数'}, 400)
            return

        if data_type not in ('ensemble', 'ensemble_mean'):
            self.send_json({'error': '无效的 type 参数'}, 400)
            return

        if model not in ('FNV3', 'GENC'):
            self.send_json({'error': '无效的 model 参数'}, 400)
            return

        # 1. Check local cache first (skip if source=mirror)
        filename = f'{model}_{data_type}_{init_time}_paired.csv'
        cache_path = os.path.join(DATA_DIR, filename)
        if source == 'auto' and os.path.exists(cache_path) and os.path.getsize(cache_path) > 1000:
            with open(cache_path, 'r', encoding='utf-8') as f:
                csv_text = f.read()
            self.send_json({
                'csv': csv_text,
                'size': len(csv_text.encode('utf-8')),
                'url': 'local_cache',
                'filename': filename,
                'cached': True,
                'source': 'local_cache'
            })
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
                        self.send_json({
                            'csv': csv_text,
                            'size': len(data),
                            'url': mirror_url,
                            'filename': filename,
                            'cached': False,
                            'source': 'github_mirror'
                        })
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
                            self.send_json({
                                'csv': csv_text,
                                'size': len(data),
                                'url': raw_url,
                                'filename': filename,
                                'cached': False,
                                'source': 'github_raw'
                            })
                            return
            except Exception as e:
                print(f'  GitHub raw fallback miss for {filename}: {e}')
                if source == 'mirror':
                    self.send_json({'error': f'GitHub镜像下载失败: {e}'}, 502)
                    return

        # 3. NAS 无梯子，不尝试 DeepMind 直连，数据由 Mac 自动化推送到 GitHub 后通过 CDN 获取
        self.send_json({'error': '该批次数据尚未同步到 GitHub，请等待自动化推送后再试'}, 404)

    def send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def endheaders_with_cors(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()

if __name__ == '__main__':
    server = http.server.HTTPServer(('0.0.0.0', PORT), TyphoonHandler)
    print(f'服务器启动: http://localhost:{PORT}')
    print(f'API: http://localhost:{PORT}/api/latest  (获取最新批次)')
    print(f'API: http://localhost:{PORT}/api/download?model=FNV3|GENC&init_time=YYYY_MM_DDTHH_00&type=ensemble')
    if GITHUB_MIRROR_BASE:
        print(f'GitHub镜像源: {GITHUB_MIRROR_BASE} (国内免梯子)')
    else:
        print(f'GitHub镜像源: 未配置 (设置环境变量 GITHUB_MIRROR_BASE 启用)')
    server.serve_forever()
