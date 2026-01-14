import hashlib
import hmac
import base64
import datetime
import requests
import json
import time
from urllib.parse import urlparse
from supabase import create_client, Client


# =========================================================================
# Pudu Client Class
# =========================================================================

class PuduClient:
    def __init__(self, app_key, app_secret, base_url="https://csu-open-platform.pudutech.com/pudu-entry"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url.rstrip('/')
        self.request_delay = 0.2

    def _get_gmt_time(self):
        return datetime.datetime.now(datetime.timezone.utc).strftime('%a, %d %b %Y %H:%M:%S GMT')

    def _calculate_content_md5(self, body):
        if not body: return ""
        md5_hash = hashlib.md5(body.encode('utf-8')).hexdigest()
        return base64.b64encode(md5_hash.encode('utf-8')).decode('utf-8')

    def _normalize_query(self, params):
        if not params: return ""
        sorted_keys = sorted(params.keys())
        parts = []
        for k in sorted_keys:
            val = params[k]
            if isinstance(val, list):
                valid_vals = [str(v) for v in val if str(v) != ""]
                if valid_vals:
                    for v in sorted(valid_vals): parts.append(f"{k}={v}")
                else:
                    parts.append(k)
            else:
                if str(val) != "":
                    parts.append(f"{k}={str(val)}")
                else:
                    parts.append(k)
        return "&".join(parts)

    def request(self, method, endpoint, params=None, body=None):
        time.sleep(self.request_delay)
        url = f"{self.base_url}{endpoint}"
        parsed_url = urlparse(url)
        path = parsed_url.path
        signing_path = path
        for prefix in ["/release", "/test", "/prepub"]:
            if signing_path.startswith(prefix):
                signing_path = "/" + signing_path[len(prefix):].lstrip('/')
                break
        if not signing_path: signing_path = "/"
        query_str = self._normalize_query(params)
        if query_str: signing_path = f"{signing_path}?{query_str}"

        content_md5 = ""
        body_str = None
        if method.upper() == "POST":
            body_str = json.dumps(body) if body else "{}"
            content_md5 = self._calculate_content_md5(body_str)

        date_str = self._get_gmt_time()
        accept = "application/json"
        content_type = "application/json"
        signing_str = f"x-date: {date_str}\n{method.upper()}\n{accept}\n{content_type}\n{content_md5}\n{signing_path}"
        hashed = hmac.new(self.app_secret.encode('utf-8'), signing_str.encode('utf-8'), hashlib.sha1)
        signature = base64.b64encode(hashed.digest()).decode('utf-8')
        auth_header = f'hmac id="{self.app_key}", algorithm="hmac-sha1", headers="x-date", signature="{signature}"'
        headers = {"Host": parsed_url.hostname, "Accept": accept, "Content-Type": content_type, "x-date": date_str,
                   "Authorization": auth_header}

        max_retries = 3
        for attempt in range(max_retries):
            try:
                if method.upper() == "GET":
                    response = requests.get(url, headers=headers, params=params, timeout=30)
                else:
                    response = requests.post(url, headers=headers, params=params, data=body_str, timeout=30)
                if response.status_code == 200: return response.json()
                if response.status_code >= 500: return None
            except requests.exceptions.RequestException:
                time.sleep(2)
        return None

    # API Wrappers
    def get_store_list(self, limit=20, offset=0):
        return self.request("GET", "/data-open-platform-service/v1/api/shop", params={"limit": limit, "offset": offset})

    def get_robots_in_store(self, shop_id, limit=50, offset=0):
        return self.request("GET", "/data-open-platform-service/v1/api/robot",
                            params={"shop_id": shop_id, "limit": limit, "offset": offset})

    def get_robot_status_v2(self, sn):
        return self.request("GET", "/open-platform-service/v2/status/get_by_sn", params={"sn": sn})

    def get_clean_robot_detail(self, sn):
        return self.request("GET", "/cleanbot-service/v1/api/open/robot/detail", params={"sn": sn})

    def get_current_map(self, sn, need_element):
        return self.request("GET", "/map-service/v1/open/current?", params={"sn": sn, "need_element": need_element})

    def get_fault_log(self, start_time, end_time, shop_id=None, limit=10, levels=None):
        params = {"start_time": int(start_time), "end_time": int(end_time), "limit": limit, "offset": 0}
        if shop_id: params["shop_id"] = shop_id
        if levels: params["error_levels"] = levels
        return self.request("GET", "/data-board/v1/log/error/query_list", params=params)

    def get_cleaning_records(self, start_time, end_time, sn=None, shop_id=None, limit=10, offset=0):
        params = {"start_time": int(start_time), "end_time": int(end_time), "limit": limit, "offset": offset}
        if sn: params["sn"] = sn
        if shop_id: params["shop_id"] = shop_id
        return self.request("GET", "/data-board/v1/log/clean_task/query_list", params=params)

    def get_cleaning_report_detail(self, sn, report_id, start_time, end_time, shop_id=None):
        params = {"sn": sn, "report_id": report_id, "start_time": int(start_time), "end_time": int(end_time),
                  "timezone_offset": 0}
        if shop_id: params["shop_id"] = shop_id
        return self.request("GET", "/data-board/v1/log/clean_task/query", params=params)


# =========================================================================
# Main Execution
# =========================================================================

def liveupdate():
    # --- PASTE CREDENTIALS HERE ---
    APP_KEY = "APIDFF0ibycU0DBmYHt06kVQHIwBiwaPyx4Bx"
    APP_SECRET = "1pL9qcpNpFhQJDFrXwjjnP5VH0J8RDQOeuqVlFMD"
    BASE_URL = "https://csu-open-platform.pudutech.com/pudu-entry"

    # --- SUPABASE CREDENTIALS (FROM YOUR CODE) ---
    SB_URL = "https://qmtbjuxqbpwirnbdkidc.supabase.co"
    SB_KEY = "sb_secret_3sHmDNdKiWkIEghnvmwa1Q_dvkRNlpP"

    client = PuduClient(APP_KEY, APP_SECRET, BASE_URL)
    sb: Client = create_client(SB_URL, SB_KEY)

    now = int(time.time())
    yesterday = now - (24 * 3600) * 7

    print(f"--- Starting Sync {datetime.datetime.now().time()} ---")

    # 1. Fetch ALL Stores
    all_stores = []
    offset = 0
    while True:
        stores_resp = client.get_store_list(limit=50, offset=offset)
        if not stores_resp or "data" not in stores_resp: break
        batch = stores_resp.get("data", {}).get("list", [])
        if not batch: break
        all_stores.extend(batch)
        if len(batch) < 50: break
        offset += 50
        time.sleep(0.1)

    print(f"Total Stores: {len(all_stores)}")

    # 2. Process Stores
    for store in all_stores:
        shop_id = store.get("shop_id")
        shop_name = store.get("shop_name")
        print(f"📍 {shop_name}")

        try:
            sb.table("shops").upsert({"shop_id": str(shop_id), "shop_name": shop_name,
                                      "last_updated": datetime.datetime.now().isoformat()}).execute()
        except Exception:
            pass

        # 3. Get Robots
        robots_resp = client.get_robots_in_store(shop_id, limit=50)
        robots = robots_resp.get("data", {}).get("list", [])

        for robot in robots:
            sn = robot.get("sn")
            model = robot.get("product_code")

            robot_data = {
                "sn": sn,
                "shop_id": str(shop_id),
                "model": model,
                "last_seen": datetime.datetime.now().isoformat()
            }

            # 4. Status (Battery/Busy)
            st = client.get_robot_status_v2(sn)
            if st and st.get("data"):
                d = st["data"]
                robot_data["status"] = d.get('run_state')
                robot_data["battery_level"] = d.get('battery')
                robot_data["is_charging"] = d.get('is_charging')

            # 5. Detail (Position / Map / Heading)
            det = client.get_clean_robot_detail(sn)
            if det and det.get("data"):
                d = det["data"]
                pos = d.get("position", {})
                map_info = d.get("map", {})
                clean = d.get("cleanbot", {})

                if map_info:
                    robot_data["map_name"] = map_info.get('name')
                    robot_data["floor_level"] = str(map_info.get('lv'))
                if pos:
                    robot_data["position_x"] = pos.get('x')
                    robot_data["position_y"] = pos.get('y')
                    robot_data["angle"] = pos.get('angle')  # This caused the angle error before
                if clean:
                    robot_data["water_level"] = clean.get('rising')
                    robot_data["sewage_level"] = clean.get('sewage')

            # 6. Map Elements (Vectors)
            # Only fetch if we successfully got map name (avoids spamming API if offline)
            if robot_data.get("map_name"):
                map_res = client.get_current_map(sn, need_element="True")
                if map_res and map_res.get('data'):
                    robot_data["map_elements"] = map_res['data'].get('elements',
                                                                     [])  # This caused the map_elements error

            # UPSERT ROBOT
            try:
                sb.table("robots").upsert(robot_data).execute()
            except Exception as e:
                print(f"   [Error Robot {sn}] {e}")

            # 7. Faults
            if shop_id:
                faults = client.get_fault_log(yesterday, now, shop_id=shop_id)
                if faults and faults.get("data"):
                    f_list = [f for f in faults["data"].get("list", []) if f.get('pid') == sn or f.get('sn') == sn]
                    seen = {}
                    for f in f_list:
                        uid = f"{sn}_{f.get('task_time')}_{f.get('error_type')}"
                        if uid not in seen:
                            seen[uid] = {
                                "fault_id": uid, "sn": sn, "shop_id": str(shop_id),
                                "error_detail": f.get("error_detail"), "error_level": f.get("error_level"),
                                "error_type": f.get("error_type"), "occurred_at": f.get("task_time")
                            }
                    if seen:
                        try:
                            sb.table("robot_faults").upsert(list(seen.values())).execute()
                        except Exception as e:
                            print(f"   [Error Faults] {e}")

    print("--- Sync Complete ---")


while True:
    liveupdate()
    time.sleep(5)