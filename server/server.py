# -*- coding: utf-8 -*-
"""
圈圈桌遊店 - 訂位與揪團 Server
==============================

一支程式同時開三個 Port：

  * HTTP      : 8000  -> 提供網頁檔案 (src/ 資料夾)，讓別台電腦用瀏覽器打開
  * WebSocket : 8765  -> 瀏覽器 (網頁 Client) 用的連線，傳 JSON 訊息
  * TCP       : 9000  -> 命令列 Client (cli_client.py) 用的原始 TCP Socket，
                         一行一個 JSON 訊息 (以 '\\n' 分隔)

兩種 Client 走同一套「指令處理」邏輯、共用同一份資料，所以網頁訂走的桌位，
CLI 也訂不到，反之亦然。

訊息格式 (Client -> Server)：
    {"id": 1, "action": "reserve", "data": {...}}
回應格式 (Server -> Client)：
    {"id": 1, "type": "response", "ok": true,  "data": {...}}
    {"id": 1, "type": "response", "ok": false, "code": "SLOT_TAKEN", "error": "..."}
主動推播 (Server -> 所有 Client)：
    {"type": "event", "event": "state", "data": {...}}

執行：
    pip install websockets
    python server.py              # 預設 0.0.0.0，所有網卡都接受連線
    python server.py --host 127.0.0.1   # 只允許本機
"""

import argparse
import asyncio
import functools
import hashlib
import http.server
import json
import os
import secrets
import socket
import threading
from datetime import datetime, date

import websockets
from websockets.asyncio.server import serve

# ---------------------------------------------------------------------------
# 基本設定
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_ROOT = os.path.normpath(os.path.join(BASE_DIR, "..", "src"))
DATA_FILE = os.path.join(BASE_DIR, "data.json")

HTTP_PORT = 8000
WS_PORT = 8765
TCP_PORT = 9000

TABLES = {            # 桌號: (名稱, 最多人數)
    "A": ("A 桌 (大桌)", 8),
    "B": ("B 桌 (小桌)", 4),
    "C": ("C 桌 (中桌)", 6),
}
TIME_SLOTS = ["14:00", "15:00", "16:00", "17:00", "18:00", "19:00", "20:00"]

ERR = {
    "BAD_JSON":       "訊息格式錯誤，無法解析 JSON",
    "UNKNOWN_ACTION": "不支援的指令",
    "INVALID_INPUT":  "輸入資料不正確",
    "NOT_LOGGED_IN":  "請先登入",
    "AUTH_FAILED":    "帳號或密碼錯誤",
    "USER_EXISTS":    "此帳號已被使用",
    "FORBIDDEN":      "權限不足，此功能僅限管理員",
    "SLOT_TAKEN":     "此桌位在該時段已被預約",
    "DUPLICATE":      "你在這個時段已經有預約了，不能重複預約",
    "OVER_CAPACITY":  "人數超過此桌可容納人數",
    "NOT_FOUND":      "找不到這筆資料",
    "LFG_FULL":       "此團已滿",
    "LFG_JOINED":     "你已經在這個團裡了",
}


def now_str():
    return datetime.now().strftime("%H:%M:%S")


def log(peer, kind, arrow, text):
    """統一的 Server 日誌格式：[時間] [協定] IP:Port 方向 內容"""
    print(f"[{now_str()}] [{kind:<3}] {peer:<21} {arrow} {text}", flush=True)


def hash_password(password, salt=None):
    """密碼不存明碼：用 PBKDF2-SHA256 + 隨機 salt"""
    salt = salt or secrets.token_hex(8)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000).hex()
    return f"{salt}${digest}"


def check_password(password, stored):
    salt, _ = stored.split("$", 1)
    return secrets.compare_digest(hash_password(password, salt), stored)


class ApiError(Exception):
    def __init__(self, code, detail=None):
        self.code = code
        self.message = ERR.get(code, code) + (f"：{detail}" if detail else "")
        super().__init__(self.message)


# ---------------------------------------------------------------------------
# 資料 (Server 是唯一的資料來源)
# ---------------------------------------------------------------------------
def default_data():
    today = date.today().isoformat()
    return {
        "next_id": 1000,
        "users": [
            {"id": 1, "username": "user", "name": "王小明", "role": "member",
             "password": hash_password("user"), "xp": 3450, "level": 8,
             "title": "白銀策略家", "preferences": ["策略", "輕策略"]},
            {"id": 2, "username": "user2", "name": "陳小美", "role": "member",
             "password": hash_password("user2"), "xp": 800, "level": 2,
             "title": "桌遊新手", "preferences": ["派對"]},
            {"id": 99, "username": "admin", "name": "店長大大", "role": "admin",
             "password": hash_password("admin"), "xp": 99999, "level": 99,
             "title": "桌遊之神", "preferences": []},
        ],
        "reservations": [
            {"id": 1, "userId": 99, "userName": "店長大大", "date": today, "table": "A",
             "time": "14:00", "party_size": 6, "note": "店內活動", "status": "已確認"},
            {"id": 2, "userId": 99, "userName": "店長大大", "date": today, "table": "A",
             "time": "15:00", "party_size": 6, "note": "店內活動", "status": "已確認"},
        ],
        "lfg": [
            {"id": 101, "host": "彬皓", "game": "阿瓦隆 (Avalon)", "maxPlayers": 8,
             "currentPlayers": ["彬皓", "皓文", "寶龍"], "date": today, "time": "17:00", "table": "A"},
            {"id": 102, "host": "店長大大", "game": "璀璨寶石 (Splendor)", "maxPlayers": 4,
             "currentPlayers": ["店長大大"], "date": today, "time": "16:00", "table": "B"},
            {"id": 103, "host": "王小明", "game": "機密代號 (Codenames)", "maxPlayers": 6,
             "currentPlayers": ["王小明", "小美", "阿強", "老李", "大明", "阿花"],
             "date": today, "time": "19:00", "table": "C"},
        ],
    }


class Store:
    def __init__(self, path):
        self.path = path
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                self.data = json.load(f)
        else:
            self.data = default_data()
            self.save()

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)

    def new_id(self):
        self.data["next_id"] += 1
        return self.data["next_id"]

    def user_by_id(self, uid):
        return next((u for u in self.data["users"] if u["id"] == uid), None)

    @staticmethod
    def public_user(u):
        return {k: v for k, v in u.items() if k != "password"}

    def public_state(self):
        """推播給所有人的公開資料：桌況 + 揪團 (不含密碼等私人資料)"""
        return {
            "tables": [{"id": k, "name": v[0], "capacity": v[1]} for k, v in TABLES.items()],
            "slots": TIME_SLOTS,
            "bookings": [
                {"id": r["id"], "date": r["date"], "table": r["table"], "time": r["time"],
                 "userId": r["userId"], "userName": r["userName"],
                 "party_size": r["party_size"], "note": r.get("note", "")}
                for r in self.data["reservations"]
            ],
            "lfg": self.data["lfg"],
            "serverTime": datetime.now().isoformat(timespec="seconds"),
        }


# ---------------------------------------------------------------------------
# 連線管理
# ---------------------------------------------------------------------------
class Session:
    """每一條連線 (不論 WebSocket 或 TCP) 都有一個 Session"""

    def __init__(self, kind, peer, sender):
        self.kind = kind          # "WS" or "TCP"
        self.peer = peer          # "IP:Port"
        self.sender = sender      # async function(dict) -> 送出訊息
        self.user_id = None


class Server:
    def __init__(self, store):
        self.store = store
        self.sessions = set()
        # token -> user_id (登入憑證)。存在 data.json，Server 重開後 Client 仍可自動恢復登入
        self.tokens = store.data.setdefault("tokens", {})
        self.lock = asyncio.Lock()

    # ---------------- 共用：處理一則訊息 ----------------
    async def handle_raw(self, session, raw):
        try:
            msg = json.loads(raw)
            if not isinstance(msg, dict):
                raise ValueError
        except (ValueError, TypeError):
            log(session.peer, session.kind, "->", f"(無法解析) {raw[:80]!r}")
            await self.send(session, {"type": "response", "id": None, "ok": False,
                                      "code": "BAD_JSON", "error": ERR["BAD_JSON"]})
            return

        req_id = msg.get("id")
        action = msg.get("action", "")
        data = msg.get("data") or {}
        log(session.peer, session.kind, "->", f"{action} {self._brief(data)}")

        changed = False
        try:
            handler = getattr(self, f"do_{action}", None)
            if not handler:
                raise ApiError("UNKNOWN_ACTION", action)
            async with self.lock:          # 同一時間只處理一個會改資料的請求，避免搶位衝突
                result, changed = handler(session, data)
                if changed:
                    self.store.save()
            reply = {"type": "response", "id": req_id, "action": action, "ok": True, "data": result}
            log(session.peer, session.kind, "<-", f"OK {action}")
        except ApiError as e:
            reply = {"type": "response", "id": req_id, "action": action, "ok": False,
                     "code": e.code, "error": e.message}
            log(session.peer, session.kind, "<-", f"FAIL {action} [{e.code}] {e.message}")

        await self.send(session, reply)
        if changed:
            await self.broadcast_state()

    @staticmethod
    def _brief(data):
        shown = {k: ("***" if "password" in k.lower() or k == "token" else v) for k, v in data.items()}
        text = json.dumps(shown, ensure_ascii=False)
        return text if len(text) < 120 else text[:117] + "..."

    async def send(self, session, obj):
        try:
            await session.sender(obj)
        except Exception:
            pass   # 對方已斷線，交給連線迴圈清理

    async def broadcast_state(self):
        msg = {"type": "event", "event": "state", "data": self.store.public_state()}
        targets = list(self.sessions)
        print(f"[{now_str()}] [ALL] 推播最新桌況給 {len(targets)} 個 Client", flush=True)
        await asyncio.gather(*(self.send(s, msg) for s in targets))

    # ---------------- 輔助 ----------------
    def require_login(self, session):
        user = self.store.user_by_id(session.user_id) if session.user_id else None
        if not user:
            raise ApiError("NOT_LOGGED_IN")
        return user

    def require_admin(self, session):
        user = self.require_login(session)
        if user["role"] != "admin":
            raise ApiError("FORBIDDEN")
        return user

    def login_session(self, session, user):
        session.user_id = user["id"]
        token = secrets.token_hex(16)
        self.tokens[token] = user["id"]
        self.store.save()
        return {"user": self.store.public_user(user), "token": token}

    # ---------------- 指令 (每個回傳 (結果, 資料是否有變更)) ----------------
    def do_ping(self, session, data):
        return {"pong": True, "serverTime": now_str()}, False

    def do_get_state(self, session, data):
        return self.store.public_state(), False

    def do_login(self, session, data):
        username = str(data.get("username", "")).strip()
        password = str(data.get("password", ""))
        user = next((u for u in self.store.data["users"] if u["username"] == username), None)
        if not user or not check_password(password, user["password"]):
            raise ApiError("AUTH_FAILED")
        return self.login_session(session, user), False

    def do_resume(self, session, data):
        """網頁重新整理或斷線重連時，用 token 恢復登入狀態"""
        uid = self.tokens.get(str(data.get("token", "")))
        user = self.store.user_by_id(uid) if uid else None
        if not user:
            raise ApiError("NOT_LOGGED_IN", "登入已過期，請重新登入")
        session.user_id = user["id"]
        return {"user": self.store.public_user(user)}, False

    def do_logout(self, session, data):
        token = str(data.get("token", ""))
        self.tokens.pop(token, None)
        session.user_id = None
        self.store.save()
        return {}, False

    def do_register(self, session, data):
        username = str(data.get("username", "")).strip()
        name = str(data.get("name", "")).strip()
        password = str(data.get("password", ""))
        if not (3 <= len(username) <= 20) or not name or len(password) < 3:
            raise ApiError("INVALID_INPUT", "帳號 3~20 字、姓名必填、密碼至少 3 碼")
        if any(u["username"] == username for u in self.store.data["users"]):
            raise ApiError("USER_EXISTS")
        user = {"id": self.store.new_id(), "username": username, "name": name, "role": "member",
                "password": hash_password(password), "xp": 0, "level": 1,
                "title": "桌遊新手", "preferences": ["派對"]}
        self.store.data["users"].append(user)
        return self.login_session(session, user), True

    def do_reserve(self, session, data):
        user = self.require_login(session)
        d = str(data.get("date", ""))
        table = str(data.get("table", "")).upper()
        time = str(data.get("time", ""))
        try:
            party = int(data.get("party_size", 0))
            day = date.fromisoformat(d)
        except (TypeError, ValueError):
            raise ApiError("INVALID_INPUT", "日期或人數格式錯誤")
        if table not in TABLES:
            raise ApiError("INVALID_INPUT", f"沒有 {table} 桌")
        if time not in TIME_SLOTS:
            raise ApiError("INVALID_INPUT", f"時段需為 {TIME_SLOTS[0]}~{TIME_SLOTS[-1]} 整點")
        if day < date.today():
            raise ApiError("INVALID_INPUT", "不能預約過去的日期")
        if party < 1:
            raise ApiError("INVALID_INPUT", "人數至少 1 人")
        cap = TABLES[table][1]
        if party > cap:
            raise ApiError("OVER_CAPACITY", f"{TABLES[table][0]} 最多 {cap} 人")

        res = self.store.data["reservations"]
        if any(r["userId"] == user["id"] and r["date"] == d and r["time"] == time for r in res):
            raise ApiError("DUPLICATE")
        taken = next((r for r in res if r["date"] == d and r["table"] == table and r["time"] == time), None)
        lfg = next((p for p in self.store.data["lfg"]
                    if p.get("date") == d and p.get("table") == table and p.get("time") == time), None)
        if taken or lfg:
            raise ApiError("SLOT_TAKEN", f"{d} {time} {table} 桌")

        r = {"id": self.store.new_id(), "userId": user["id"], "userName": user["name"],
             "date": d, "table": table, "time": time, "party_size": party,
             "note": str(data.get("note", ""))[:100], "status": "已確認"}
        res.append(r)
        return {"reservation": r}, True

    def do_cancel_reservation(self, session, data):
        user = self.require_login(session)
        rid = data.get("id")
        res = self.store.data["reservations"]
        r = next((x for x in res if x["id"] == rid), None)
        if not r:
            raise ApiError("NOT_FOUND")
        if r["userId"] != user["id"] and user["role"] != "admin":
            raise ApiError("FORBIDDEN", "只能取消自己的預約")
        res.remove(r)
        return {"cancelled": rid}, True

    def do_my_reservations(self, session, data):
        user = self.require_login(session)
        mine = [r for r in self.store.data["reservations"] if r["userId"] == user["id"]]
        return {"reservations": mine}, False

    def do_lfg_join(self, session, data):
        user = self.require_login(session)
        post = next((p for p in self.store.data["lfg"] if p["id"] == data.get("id")), None)
        if not post:
            raise ApiError("NOT_FOUND")
        if user["name"] in post["currentPlayers"]:
            raise ApiError("LFG_JOINED")
        if len(post["currentPlayers"]) >= post["maxPlayers"]:
            raise ApiError("LFG_FULL")
        post["currentPlayers"].append(user["name"])
        return {"post": post}, True

    def do_lfg_leave(self, session, data):
        user = self.require_login(session)
        post = next((p for p in self.store.data["lfg"] if p["id"] == data.get("id")), None)
        if not post or user["name"] not in post["currentPlayers"]:
            raise ApiError("NOT_FOUND")
        post["currentPlayers"].remove(user["name"])
        return {"post": post}, True

    def do_admin_list_reservations(self, session, data):
        self.require_admin(session)
        return {"reservations": self.store.data["reservations"]}, False

    def do_admin_list_users(self, session, data):
        self.require_admin(session)
        return {"users": [self.store.public_user(u) for u in self.store.data["users"]]}, False

    def do_admin_kick_user(self, session, data):
        self.require_admin(session)
        u = self.store.user_by_id(data.get("id"))
        if not u:
            raise ApiError("NOT_FOUND")
        if u["role"] == "admin":
            raise ApiError("FORBIDDEN", "不能踢除管理員")
        self.store.data["users"].remove(u)
        self.store.data["reservations"] = [r for r in self.store.data["reservations"] if r["userId"] != u["id"]]
        for t, uid in list(self.tokens.items()):
            if uid == u["id"]:
                del self.tokens[t]
        for s in self.sessions:
            if s.user_id == u["id"]:
                s.user_id = None
        return {"kicked": u["id"]}, True

    # ---------------- WebSocket 連線 (瀏覽器) ----------------
    async def ws_handler(self, ws):
        peer = "%s:%s" % ws.remote_address[:2]

        async def sender(obj):
            await ws.send(json.dumps(obj, ensure_ascii=False))

        session = Session("WS", peer, sender)
        self.sessions.add(session)
        log(peer, "WS", "++", f"WebSocket 連線建立 (目前 {len(self.sessions)} 個 Client)")
        try:
            await sender({"type": "event", "event": "welcome",
                          "data": {"message": "已連線到圈圈桌遊店 Server", "yourAddress": peer}})
            async for raw in ws:
                await self.handle_raw(session, raw)
        except websockets.ConnectionClosed:
            pass
        finally:
            self.sessions.discard(session)
            log(peer, "WS", "--", f"連線中斷 (剩 {len(self.sessions)} 個 Client)")

    # ---------------- 原始 TCP 連線 (CLI Client) ----------------
    async def tcp_handler(self, reader, writer):
        addr = writer.get_extra_info("peername")
        peer = "%s:%s" % addr[:2]

        async def sender(obj):
            writer.write((json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))
            await writer.drain()

        session = Session("TCP", peer, sender)
        self.sessions.add(session)
        log(peer, "TCP", "++", f"TCP 連線建立 (目前 {len(self.sessions)} 個 Client)")
        try:
            await sender({"type": "event", "event": "welcome",
                          "data": {"message": "已連線到圈圈桌遊店 Server", "yourAddress": peer}})
            while True:
                line = await reader.readline()        # 以 '\n' 切出一則訊息
                if not line:                          # 收到 0 bytes = 對方關閉連線 (FIN)
                    break
                text = line.decode("utf-8", errors="replace").strip()
                if text:
                    await self.handle_raw(session, text)
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass                                      # Client 異常中斷 (RST)
        finally:
            self.sessions.discard(session)
            log(peer, "TCP", "--", f"連線中斷 (剩 {len(self.sessions)} 個 Client)")
            writer.close()


# ---------------------------------------------------------------------------
# HTTP 靜態網頁 (另開一條執行緒)
# ---------------------------------------------------------------------------
class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        if self.path in ("/", "/index.html"):
            peer = "%s:%s" % self.client_address[:2]
            log(peer, "WEB", "->", f"GET {self.path} (下載網頁)")


def start_http(host):
    handler = functools.partial(QuietHandler, directory=WEB_ROOT)
    httpd = http.server.ThreadingHTTPServer((host, HTTP_PORT), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd


def local_ips():
    ips = set()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))       # UDP 不會真的送出封包，只是查本機對外 IP
        ips.add(s.getsockname()[0])
        s.close()
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass
    ips.discard("127.0.0.1")
    return sorted(ips) or ["127.0.0.1"]


async def main(host):
    store = Store(DATA_FILE)
    server = Server(store)

    start_http(host)
    tcp_server = await asyncio.start_server(server.tcp_handler, host, TCP_PORT)

    async with serve(server.ws_handler, host, WS_PORT):
        print("=" * 64)
        print(" 圈圈桌遊店 Server 已啟動")
        print(f" 監聽位址 {host}  |  HTTP {HTTP_PORT}  |  WebSocket {WS_PORT}  |  TCP {TCP_PORT}")
        print(" 其他電腦請連：")
        for ip in local_ips():
            print(f"   網頁  ->  http://{ip}:{HTTP_PORT}")
            print(f"   CLI   ->  python cli_client.py --host {ip}")
        print(f" 本機測試 ->  http://127.0.0.1:{HTTP_PORT}")
        print(" 按 Ctrl+C 關閉 Server")
        print("=" * 64, flush=True)
        async with tcp_server:
            await asyncio.Future()   # 永遠執行


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="圈圈桌遊店 Server")
    ap.add_argument("--host", default="0.0.0.0", help="監聽的 IP，預設 0.0.0.0 (所有網卡)")
    ap.add_argument("--reset", action="store_true", help="清除 data.json，回到預設資料")
    args = ap.parse_args()
    if args.reset and os.path.exists(DATA_FILE):
        os.remove(DATA_FILE)
        print("已清除 data.json")
    try:
        asyncio.run(main(args.host))
    except KeyboardInterrupt:
        print("\nServer 已關閉")
