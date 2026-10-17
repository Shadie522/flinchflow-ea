import os
import asyncio
import json
import logging
import random
import threading
import websockets
from http.server import HTTPServer, BaseHTTPRequestHandler
from telegram import Bot

# Enable logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)

# Configuration & Credentials (Pulled securely from Render Environment Variables)
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")
DERIV_API_TOKEN = os.environ.get("DERIV_API_TOKEN")
WS_URL = "wss://ws.derivws.com/websockets/v3?app_id=1089"

# Portfolio Risk Management Limits
DAILY_PROFIT_TARGET = 60.0  
DAILY_STOP_LOSS = 30.0    

bot = Bot(token=TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
shared_pnl = 0.0
initial_balance = None
is_running = True

async def send_telegram_alert(message):
    if not bot:
        return
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
    except Exception as e:
        logging.error("Telegram alert failed: %s", e)

# --- HEALTH CHECK SERVER ---
PORT = int(os.environ.get("PORT", 10000))

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"FlinchFlow EA Multi-Asset Bot is active!")
    def log_message(self, format, *args):
        return

def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthCheckHandler)
    server.serve_forever()
# --------------------------

def calculate_ema(prices, period):
    if len(prices) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for price in prices[period:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    gains, losses = 0, 0
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        if change > 0:
            gains += change
        else:
            losses -= change
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    return 100 - (100 / (1 + (avg_gain / avg_loss)))

async def run_trading_bot():
    global is_running, shared_pnl, initial_balance
    
    price_histories = {
        "R_100": [],
        "frxXAUUSD": [],
        "cryBTCUSD": []
    }
    stakes = {
        "R_100": 10,
        "frxXAUUSD": 10,
        "cryBTCUSD": 5
    }
    cooldowns = {
        "R_100": 0,
        "frxXAUUSD": 0,
        "cryBTCUSD": 0
    }
    
    backoff = 3

    while is_running:
        try:
            logging.info("Connecting to Deriv via single unified WebSocket...")
            # Clean connection without extra_headers blocking Render's event loop
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as websocket:
                backoff = 3  
                
                # 1. Authorize
                await websocket.send(json.dumps({"authorize": DERIV_API_TOKEN}))
                auth_res = json.loads(await websocket.recv())
                if "error" in auth_res:
                    logging.error("Authorization failed: %s", auth_res['error']['message'])
                    await asyncio.sleep(15)
                    continue
                
                logging.info("Authorized successfully! Subscribing to balance and markets...")
                
                # 2. Subscribe to Balance
                await websocket.send(json.dumps({"balance": 1, "subscribe": 1}))
                
                # 3. Subscribe to all 3 markets on the same connection
                for symbol in price_histories.keys():
                    await websocket.send(json.dumps({"ticks": symbol, "subscribe": 1}))
                    await asyncio.sleep(0.5)
                
                online_msg = "🌐 *Multi-Asset Single-Connection EA Online*\n• Markets: `Volatility 100`, `Gold`, `Bitcoin`\n• Target: +${} \vert{} Stop Loss: -${}".format(DAILY_PROFIT_TARGET, DAILY_STOP_LOSS)
                await send_telegram_alert(online_msg)

                # 4. Main Event Loop for incoming messages
                async for message in websocket:
                    if not is_running:
                        break
                    
                    data = json.loads(message)
                    msg_type = data.get("msg_type")
                    
                    # Handle Balance / Risk Management
                    if msg_type == "balance":
                        balance_val = float(data["balance"]["balance"])
                        if initial_balance is None:
                            initial_balance = balance_val
                        
                        shared_pnl = balance_val - initial_balance
                        if shared_pnl >= DAILY_PROFIT_TARGET:
                            target_msg = "🎯 Target Reached (+${:.2f})! Shutting down.".format(shared_pnl)
                            await send_telegram_alert(target_msg)
                            is_running = False
                            break
                        elif shared_pnl <= -DAILY_STOP_LOSS:
                            sl_msg = "🛑 Stop Loss Hit (-${:.2f})! Shutting down.".format(abs(shared_pnl))
                            await send_telegram_alert(sl_msg)
                            is_running = False
                            break

                    # Handle Market Ticks & Strategy Execution
                    elif msg_type == "tick":
                        tick = data["tick"]
                        symbol = tick["symbol"]
                        price = float(tick["quote"])
                        
                        if symbol in price_histories:
                            history = price_histories[symbol]
                            history.append(price)
                            
                            if len(history) > 30:
                                history.pop(0)
                                
                            if cooldowns[symbol] > 0:
                                cooldowns[symbol] -= 1
                                continue
                                
                            if len(history) >= 20:
                                fast_ema = calculate_ema(history, 5)
                                slow_ema = calculate_ema(history, 12)
                                rsi_val = calculate_rsi(history, 14)
                                
                                action_type = None
                                if fast_ema and slow_ema and fast_ema > slow_ema and rsi_val < 35:
                                    action_type = "CALL"
                                elif fast_ema and slow_ema and fast_ema < slow_ema and rsi_val > 65:
                                    action_type = "PUT"
                                    
                                if action_type:
                                    stake_amount = stakes[symbol]
                                    trade_payload = {
                                        "buy": 1,
                                        "price": stake_amount,
                                        "parameters": {
                                            "amount": stake_amount,
                                            "basis": "stake",
                                            "contract_type": action_type,
                                            "currency": "USD",
                                            "duration": 5,
                                            "duration_unit": "t",
                                            "symbol": symbol
                                        }
                                    }
                                    await websocket.send(json.dumps(trade_payload))
                                    trade_res = json.loads(await websocket.recv())
                                    
                                    if "buy" in trade_res:
                                        contract_id = trade_res["buy"]["contract_id"]
                                        payout = trade_res["buy"]["payout"]
                                        trade_msg = "📈 *EA Trade Executed*\n• Asset: `{}`\n• Action: `{}`\n• ID: `{}`\n• Payout: `${}`".format(symbol, action_type, contract_id, payout)
                                        await send_telegram_alert(trade_msg)
                                        cooldowns[symbol] = 12

        except Exception as e:
            logging.error("WebSocket connection drop: %s. Reconnecting in %.1fs...", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.5, 30) + random.uniform(0.5, 1.5)

if __name__ == "__main__":
    server_thread = threading.Thread(target=start_health_server, daemon=True)
    server_thread.start()
    
    try:
        asyncio.run(run_trading_bot())
    except KeyboardInterrupt:
        print("Bot stopped.")