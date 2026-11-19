# -*- coding: utf-8 -*-
"""
圈圈桌遊店 - 命令列 Client (原始 TCP Socket 版)
================================================

使用 Python 內建 socket 模組 (課本 2.7 節 TCPClient 的寫法) 連到 Server 的 TCP 9000 port。
訊息為「一行一個 JSON」，以 '\\n' 當作訊息邊界 (因為 TCP 是 byte stream，沒有訊息邊界)。

執行：
    python cli_client.py                       # 連本機
    python cli_client.py --host 192.168.1.23   # 連另一台電腦上的 Server
"""

import argparse
import json
import queue
import socket
import sys
import threading
from datetime import date, datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def ts():
    return datetime.now().strftime("%H:%M:%S")


class TcpClient:
    def __init__(self, host, port, verbose=True):
        self.host, self.port, self.verbose = host, port, verbose
        self.sock = None
        self.req_id = 0
        self.responses = queue.Queue()
        self.state = None
        self.user = None
        self.alive = False

    # ---------- 連線 ----------
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)   # IPv4 + TCP
        self.sock.settimeout(5)                                         # 連線逾時 5 秒
        self.sock.connect((self.host, self.port))                       # 三向交握
        self.sock.settimeout(None)
        self.alive = True
        local = "%s:%s" % self.sock.getsockname()[:2]
        print(f"[{ts()}] 已連線  本機 {local}  ->  Server {self.host}:{self.port}")
        threading.Thread(target=self._recv_loop, daemon=True).start()

    def _recv_loop(self):
        buf = b""
        try:
            while True:
                chunk = self.sock.recv(4096)
                if not chunk:           # 0 bytes：Server 關閉連線
                    break
                buf += chunk
                while b"\n" in buf:     # 依 '\n' 切出完整訊息 (處理黏包/拆包)
                    line, buf = buf.split(b"\n", 1)
                    if line.strip():
                        self._on_message(json.loads(line.decode("utf-8")))
        except OSError:
            pass
        self.alive = False
        print(f"\n[{ts()}] !! 與 Server 的連線已中斷 (Server 關閉或網路斷線)")
        self.responses.put(None)

    def _on_message(self, msg):
        if msg.get("type") == "event":
            if msg["event"] == "state":
                self.state = msg["data"]
                print(f"\n[{ts()}] << 推播：桌況/揪團已更新 (其他 Client 有操作)")
            elif msg["event"] == "welcome":
                print(f"[{ts()}] << {msg['data']['message']} (Server 看到你是 {msg['data']['yourAddress']})")
        else:
            self.responses.put(msg)

    def request(self, action, data=None):
        if not self.alive:
            raise ConnectionError("尚未連線")
        self.req_id += 1
        msg = {"id": self.req_id, "action": action, "data": data or {}}
        raw = json.dumps(msg, ensure_ascii=False)
        if self.verbose:
            shown = raw if "password" not in raw else raw.split('"password"')[0] + '"password": "***"}}'
            print(f"[{ts()}] >> 送出 {shown}")
        self.sock.sendall((raw + "\n").encode("utf-8"))
        try:
            resp = self.responses.get(timeout=5)
        except queue.Empty:
            raise TimeoutError("Server 5 秒內沒有回應")
        if resp is None:
            raise ConnectionError("連線已中斷")
        if self.verbose:
            status = "成功" if resp.get("ok") else f"失敗 [{resp.get('code')}]"
            print(f"[{ts()}] << 回應 {status}")
        return resp

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# 介面
# ---------------------------------------------------------------------------
def show_tables(c, day):
    r = c.request("get_state")
    st = r["data"]
    c.state = st
    print(f"\n=== {day} 桌況 ===  (○ 空桌  ● 已預約  ◎ 揪團中)")
    print("桌號".ljust(14) + "".join(s.ljust(7) for s in st["slots"]))
    for t in st["tables"]:
        row = f"{t['id']} ({t['capacity']}人)".ljust(14)
        for s in st["slots"]:
            mark = "○"
            if any(b["date"] == day and b["table"] == t["id"] and b["time"] == s for b in st["bookings"]):
                mark = "●"
            elif any(p.get("date") == day and p.get("table") == t["id"] and p.get("time") == s for p in st["lfg"]):
                mark = "◎"
            row += mark.ljust(7)
        print(row)


def show_result(r, ok_text):
    if r["ok"]:
        print(f"  ✔ {ok_text}")
    else:
        print(f"  ✘ {r['error']}")


def menu(c):
    while True:
        who = c.user["name"] if c.user else "未登入"
        print(f"\n------ 圈圈桌遊店 CLI ({who}) ------")
        print(" 1. 查看桌況      2. 登入")
        print(" 3. 預約桌位      4. 我的預約 / 取消")
        print(" 5. 揪團列表 / 加入")
        print(" 9. 送出錯誤格式訊息 (測試異常)")
        print(" 0. 離開")
        choice = input("請選擇：").strip()
        try:
            if choice == "1":
                day = input(f"  日期 (Enter = 今天 {date.today()})：").strip() or date.today().isoformat()
                show_tables(c, day)
            elif choice == "2":
                u = input("  帳號：").strip()
                p = input("  密碼：").strip()
                r = c.request("login", {"username": u, "password": p})
                if r["ok"]:
                    c.user = r["data"]["user"]
                show_result(r, f"登入成功，歡迎 {c.user['name'] if c.user else ''}")
            elif choice == "3":
                day = input(f"  日期 (Enter = 今天)：").strip() or date.today().isoformat()
                table = input("  桌號 (A/B/C)：").strip().upper()
                time = input("  時段 (例 18:00)：").strip()
                party = input("  人數：").strip()
                r = c.request("reserve", {"date": day, "table": table, "time": time,
                                          "party_size": party, "note": "CLI 預約"})
                show_result(r, f"預約成功！{day} {time} {table} 桌")
            elif choice == "4":
                r = c.request("my_reservations")
                if not r["ok"]:
                    show_result(r, "")
                    continue
                mine = r["data"]["reservations"]
                if not mine:
                    print("  (目前沒有預約)")
                    continue
                for x in mine:
                    print(f"  #{x['id']}  {x['date']} {x['time']}  {x['table']} 桌  {x['party_size']} 人")
                rid = input("  要取消的編號 (Enter 略過)：").strip()
                if rid:
                    r = c.request("cancel_reservation", {"id": int(rid)})
                    show_result(r, f"已取消 #{rid}")
            elif choice == "5":
                st = c.request("get_state")["data"]
                for p in st["lfg"]:
                    n, m = len(p["currentPlayers"]), p["maxPlayers"]
                    print(f"  #{p['id']}  {p['time']} {p['table']}桌  {p['game']:<22} {n}/{m} 人  主揪 {p['host']}")
                pid = input("  要加入的編號 (Enter 略過)：").strip()
                if pid:
                    r = c.request("lfg_join", {"id": int(pid)})
                    show_result(r, "加入成功！")
            elif choice == "9":
                print(f"[{ts()}] >> 送出 'hello server' (不是 JSON)")
                c.sock.sendall(b"hello server\n")
                r = c.responses.get(timeout=5)
                print(f"[{ts()}] << 回應 失敗 [{r.get('code')}] {r.get('error')}")
            elif choice == "0":
                break
        except (ConnectionError, TimeoutError, OSError) as e:
            print(f"  ✘ 連線問題：{e}")
            if not c.alive:
                break
        except ValueError:
            print("  ✘ 輸入格式錯誤")


def main():
    ap = argparse.ArgumentParser(description="圈圈桌遊店 CLI Client")
    ap.add_argument("--host", default="127.0.0.1", help="Server IP (預設 127.0.0.1)")
    ap.add_argument("--port", type=int, default=9000, help="Server TCP Port (預設 9000)")
    args = ap.parse_args()

    c = TcpClient(args.host, args.port)
    try:
        c.connect()
    except socket.timeout:
        print(f"✘ 連線逾時：{args.host}:{args.port} 沒有回應 (IP 錯誤、不同網段或被防火牆擋住？)")
        return
    except ConnectionRefusedError:
        print(f"✘ 連線被拒絕：{args.host}:{args.port} 沒有程式在監聽 (Server 沒開？Port 打錯？)")
        return
    except OSError as e:
        print(f"✘ 無法連線：{e}")
        return
    try:
        menu(c)
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        c.close()
        print("已離開")


if __name__ == "__main__":
    main()
