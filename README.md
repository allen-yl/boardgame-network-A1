# 圈圈桌遊店 — 訂位與揪團系統 (Client–Server 版)

資訊網路 第一次作業：以 Socket 通訊為核心的 Client–Server 程式。
原本只有前端的圈圈桌遊店網站，改由 **Python Server** 統一管理會員、桌位預約與揪團名額，
多個 Client 同時操作時由 Server 判斷衝突，並即時推播最新桌況給所有連線中的 Client。

## 架構

```
          ┌───────────────────────── Server 主機 ─────────────────────────┐
          │  server/server.py  (Python 3 + asyncio)                        │
 瀏覽器 ──┼─ HTTP 8000      → 下載網頁 (src/)                              │
 (網頁)  ─┼─ WebSocket 8765 → JSON 訊息：登入/預約/揪團…，並接收推播        │
 CLI   ───┼─ TCP 9000       → 一行一個 JSON (socket 模組)                  │
          │        ↓ 同一套指令處理 + asyncio.Lock                         │
          │  server/data.json  (會員、預約、揪團、登入 token)              │
          └────────────────────────────────────────────────────────────────┘
```

| 元件 | 檔案 | 說明 |
|---|---|---|
| Server | `server/server.py` | HTTP + WebSocket + TCP 三個 Port，驗證、衝突檢查、推播、日誌 |
| 網頁 Client | `src/index.html`、`src/js/net.js`、`src/js/script.js` | 原網站介面，資料改向 Server 存取；左下角有連線燈號與封包紀錄 |
| CLI Client | `server/cli_client.py` | Python `socket` 模組寫的命令列 Client |

## 執行環境

- Python 3.10 以上 (Windows / macOS / Linux 皆可)
- 套件：`websockets` (`pip install -r server/requirements.txt`)
- 瀏覽器：Chrome / Edge (網頁會從 CDN 載入 Tailwind 與 Lucide，**Client 電腦需要能上網**)

## 啟動

### Server (A 電腦)

```bash
cd server
pip install -r requirements.txt
python server.py            # 監聽 0.0.0.0，其他電腦可連
python server.py --reset    # 清除 data.json，回到預設資料 (錄影前建議執行)
```

Windows 也可以直接雙擊 `server/start_server.bat`。
啟動後畫面會列出本機 IP，例如 `http://192.168.43.10:8000`。

> 第一次執行時 Windows 會跳出防火牆詢問，請勾選「私人網路」並允許。

### 網頁 Client (B 電腦)

瀏覽器打開 `http://<A 電腦的 IP>:8000`。左下角燈號變綠色「● 已連線」即成功，點燈號可展開封包紀錄。

### CLI Client (B 電腦或任一台)

```bash
python cli_client.py --host <A 電腦的 IP>
```

## 測試帳號

| 帳號 | 密碼 | 身分 |
|---|---|---|
| user | user | 會員 王小明 |
| user2 | user2 | 會員 陳小美 |
| admin | admin | 管理員 店長大大 |

## 功能 (已完成)

1. 註冊 / 登入 / 登出 (Server 驗證，密碼以 PBKDF2 雜湊儲存，前端不再放任何密碼)
2. 查詢桌況 (A/B/C 三桌 × 14:00–20:00)
3. 預約桌位、取消自己的預約
4. 線上揪團：加入、退出 (名額由 Server 控管)
5. 即時推播：任一 Client 改動資料，所有 Client 畫面自動更新
6. 後台 (admin)：查看全部預約、取消預約、踢除會員
7. 異常處理：

| 情況 | 處理 |
|---|---|
| 帳密錯誤 | `AUTH_FAILED` |
| 未登入就預約 | `NOT_LOGGED_IN` |
| 同桌同時段已被訂走 | `SLOT_TAKEN` |
| 同一人同時段重複預約 | `DUPLICATE` |
| 人數超過桌子容量 | `OVER_CAPACITY` |
| 揪團已滿 / 已在團內 | `LFG_FULL` / `LFG_JOINED` |
| 一般會員呼叫後台功能 | `FORBIDDEN` |
| 非 JSON 訊息 | `BAD_JSON` |
| Server 關閉 / 網路中斷 | 網頁燈號變紅、每 1→2→4→8 秒自動重連；重連後用 token 自動恢復登入 |
| Server 沒開 / IP 打錯 | CLI 顯示「連線被拒絕」或「連線逾時」 |
| 5 秒內沒回應 | `TIMEOUT` |

## 尚未完成 (Final Project 規劃)

- 桌遊清單、庫存管理仍存在瀏覽器 localStorage，尚未改由 Server 管理
- 發起揪團、客服聊天室改為多人即時聊天
- HTTPS / WSS 加密連線

## 通訊協定

Client → Server

```json
{"id": 7, "action": "reserve", "data": {"date": "2026-11-20", "table": "A", "time": "18:00", "party_size": 4, "note": ""}}
```

Server → Client (回應)

```json
{"id": 7, "type": "response", "action": "reserve", "ok": false, "code": "SLOT_TAKEN", "error": "此桌位在該時段已被預約：2026-11-20 18:00 A 桌"}
```

Server → 所有 Client (推播)

```json
{"type": "event", "event": "state", "data": {"tables": [...], "slots": [...], "bookings": [...], "lfg": [...]}}
```

支援的 action：`ping`、`get_state`、`login`、`register`、`resume`、`logout`、`reserve`、`cancel_reservation`、
`my_reservations`、`lfg_join`、`lfg_leave`、`admin_list_reservations`、`admin_list_users`、`admin_kick_user`
