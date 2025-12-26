import asyncio
import aiohttp
import json
import random
import time
import logging
from collections import defaultdict
import os

# 환경 변수 및 상수 설정
FALLBACK_PRICE = int(os.getenv("FALLBACK_PRICE", 32000))
TICKSIZE = int(os.getenv("TICKSIZE", 10))
WHALE_RATIO = float(os.getenv("WHALE_RATIO", 0.07))
SYMBOL = os.getenv("SYMBOL", "EXC")
SSE_URL = os.getenv("SSE_URL", f"http://localhost:8000/api/sse/data/{SYMBOL}?partial_book=true")
ORDER_API_BASE = os.getenv("ORDER_API_BASE", "http://localhost:8000/api/v1/market/orders")
SPREAD_FILLER_THRESHOLD = int(os.getenv("SPREAD_FILLER_THRESHOLD", 10))
ONEWAY_PROB = float(os.getenv("ONEWAY_PROB", 0.008))
CANCEL_TOP_PROB = float(os.getenv("CANCEL_TOP_PROB", 0.05))
ONEWAY_DURATION_MIN = int(os.getenv("ONEWAY_DURATION_MIN", 20))
ONEWAY_DURATION_MAX = int(os.getenv("ONEWAY_DURATION_MAX", 120))
MIN_PRICE = int(os.getenv("MIN_PRICE", 10))
UPWARD_BIAS = float(os.getenv("UPWARD_BIAS", 0.5))
MARKET_WARMUP_SECONDS = int(os.getenv("MARKET_WARMUP_SECONDS", 60))

GAP_PROBABILITY = float(os.getenv("GAP_PROBABILITY", 0.3))
GAP_MIN_TICKS = int(os.getenv("GAP_MIN_TICKS", -500))
GAP_MAX_TICKS = int(os.getenv("GAP_MAX_TICKS", 1000))

PRICE_SAVE_FILE = os.getenv("PRICE_SAVE_FILE", "last_price.json")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "trading_bot.log")
HEADERS = {}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler()
    ]
)

def aggregate_orders(orders):
    # 동일한 사이드, 타입, 가격의 주문을 하나로 합침
    # cancel_after 속성도 처리하도록 추가
    market_orders = defaultdict(lambda: {"quantity": 0, "log": None, "persistent": False, "cancel_after": None})
    limit_orders = defaultdict(lambda: {"quantity": 0, "log": None, "persistent": False, "cancel_after": None})
    
    for order in orders:
        side = order["side"]
        order_type = order["type"]
        quantity = order["quantity"]
        log_flag = order.get("log")
        persistent = order.get("persistent", False)
        cancel_after = order.get("cancel_after") # 자동 취소 시간
        
        if order_type == "market":
            key = side
            market_orders[key]["quantity"] += quantity
            if log_flag:
                market_orders[key]["log"] = log_flag
            if persistent:
                market_orders[key]["persistent"] = True
            if cancel_after:
                market_orders[key]["cancel_after"] = cancel_after
        else:
            price = order["price"]
            key = (side, price)
            limit_orders[key]["quantity"] += quantity
            if log_flag:
                limit_orders[key]["log"] = log_flag
            if persistent:
                limit_orders[key]["persistent"] = True
            if cancel_after:
                limit_orders[key]["cancel_after"] = cancel_after
    
    aggregated = []
    
    for side, data in market_orders.items():
        aggregated.append({
            "side": side,
            "type": "market",
            "quantity": data["quantity"],
            "log": data["log"],
            "persistent": data["persistent"],
            "cancel_after": data["cancel_after"]
        })
    
    for (side, price), data in limit_orders.items():
        aggregated.append({
            "side": side,
            "type": "limit",
            "price": price,
            "quantity": data["quantity"],
            "log": data["log"],
            "persistent": data["persistent"],
            "cancel_after": data["cancel_after"]
        })
    
    return aggregated


class UltraFastMarketBot:
    def __init__(self, fallback_price, ticksize, whale_ratio=0.25):
        self.depth_data = None
        self.ledger_data = None
        self.session_data = None
        self.last_trade_price = None
        self.fallback_price = fallback_price
        self.ticksize = ticksize
        self.whale_ratio = whale_ratio
        self.market_mode = "neutral"
        self.market_mode_until = 0
        self.oneway_strength = "none"
        self.min_price = MIN_PRICE
        self.liquidity_level = "normal"
        
        self.prev_market_mode = "neutral"
        self.prev_oneway_strength = "none"
        self.prev_market_trend = None
        
        self.market_opened_at = None
        self.last_close_price = None
        
        self.load_last_price()

        self.market_open_event = asyncio.Event()
        self.market_open_event.set()
        
    def save_last_price(self):
        try:
            data = {
                "last_trade_price": self.last_trade_price,
                "last_close_price": self.last_close_price,
                "timestamp": time.time()
            }
            with open(PRICE_SAVE_FILE, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logging.error(f"가격 저장 실패: {e}")

    def load_last_price(self):
        try:
            with open(PRICE_SAVE_FILE, 'r') as f:
                data = json.load(f)
                self.last_trade_price = data.get("last_trade_price")
                self.last_close_price = data.get("last_close_price")
                timestamp = data.get("timestamp")
                
                if self.last_trade_price:
                    logging.info(f"저장된 가격 로드: {self.last_trade_price}")
        except FileNotFoundError:
            logging.info("저장된 가격 파일 없음. 기본값 사용")
        except Exception as e:
            logging.error(f"가격 로드 실패: {e}")

    def protect_min_price(self, price):
        return max(price, self.min_price)

    def nearest_tick(self, price):
        protected_price = max(price, self.min_price)
        ts = self.ticksize
        return int(round(protected_price / ts) * ts)

    def get_reference_price(self):
        ltp = self.last_trade_price
        if ltp is not None:
            return self.nearest_tick(ltp)
        return self.nearest_tick(self.fallback_price)

    def is_warmup_period(self):
        if self.market_opened_at is None:
            return False
        return (time.time() - self.market_opened_at) < MARKET_WARMUP_SECONDS

    def set_liquidity_level(self):
        strength = self.oneway_strength
        if strength == "strong":
            self.liquidity_level = "high"
        elif strength == "medium":
            self.liquidity_level = "normal"
        elif strength == "weak":
            self.liquidity_level = "low"
        else:
            self.liquidity_level = random.choices(["normal", "low", "high"], [0.6, 0.25, 0.15])[0]

    def get_trading_interval(self):
        if self.is_warmup_period():
            return random.uniform(0.8, 1.2)
        
        intervals = self._intervals if hasattr(self, '_intervals') else {
            "low": (0.6, 0.8),
            "normal": (0.4, 0.6),
            "high": (0.25, 0.4)
        }
        self._intervals = intervals
        return random.uniform(*intervals[self.liquidity_level])

    def whale_orders(self, ref_price):
        direction = random.choice(["bullish", "bearish"])
        size_base = random.randint(250, 500)
        mult = random.randint(2, 4)
        flag = "[WHALE]"
        orders = []
        
        logging.info(f"고래 활동 감지: {direction}, 기준수량={size_base}, 배수={mult}")
        
        if direction == "bullish":
            orders += [{"side": "buy", "type": "market", "quantity": size_base, "log": flag} for _ in range(mult)]
            orders += [{"side": "buy", "type": "limit", "price": self.nearest_tick(ref_price + random.randint(5, 25) * (i + 1)), "quantity": random.randint(size_base // 2, size_base), "log": flag} for i in range(mult)]
            orders += [{"side": "sell", "type": "limit", "price": self.nearest_tick(ref_price + random.randint(30, 80) * (i + 1)), "quantity": random.randint(size_base // 3, size_base), "log": flag} for i in range(mult)]
        else:
            orders += [{"side": "sell", "type": "market", "quantity": size_base, "log": flag} for _ in range(mult)]
            orders += [{"side": "sell", "type": "limit", "price": self.nearest_tick(ref_price - random.randint(5, 25) * (i + 1)), "quantity": random.randint(size_base // 2, size_base), "log": flag} for i in range(mult)]
            orders += [{"side": "buy", "type": "limit", "price": self.nearest_tick(ref_price - random.randint(30, 80) * (i + 1)), "quantity": random.randint(size_base // 3, size_base), "log": flag} for i in range(mult)]
        return orders

    def maybe_trigger_oneway(self):
        now = time.time()
        
        if self.market_opened_at is not None:
            time_since_open = now - self.market_opened_at
            if time_since_open < MARKET_WARMUP_SECONDS:
                if self.market_mode != "neutral":
                    remaining = int(MARKET_WARMUP_SECONDS - time_since_open)
                    logging.info(f"시장 상태 변경: NEUTRAL (호가 채우기 - 남은 시간: {remaining}초)")
                self.market_mode = "neutral"
                self.oneway_strength = "none"
                self.prev_market_mode = "neutral"
                self.prev_oneway_strength = "none"
                return
        
        if self.market_mode == "neutral" or now > self.market_mode_until:
            if random.random() < ONEWAY_PROB:
                current_price = self.get_reference_price()
                if current_price <= 10:
                    bias = 0.9
                elif current_price <= 50:
                    bias = 0.8
                else:
                    bias = UPWARD_BIAS

                direction = random.choices(["oneway_up", "oneway_down"], [bias, 1 - bias])[0]
                self.market_mode = direction
                self.market_mode_until = now + random.randint(ONEWAY_DURATION_MIN, ONEWAY_DURATION_MAX)

                if direction == "oneway_up":
                    self.oneway_strength = random.choices(["weak", "medium", "strong"], [0.5, 0.3, 0.2])[0]
                else:
                    self.oneway_strength = random.choices(["weak", "medium", "strong"], [0.5, 0.3, 0.2])[0]
                
                duration = int(self.market_mode_until - now)
                logging.info(f"시장 상태 변경: {direction} (강도: {self.oneway_strength}, 지속: {duration}초)")
                
                self.prev_market_mode = self.market_mode
                self.prev_oneway_strength = self.oneway_strength
            else:
                if self.market_mode != "neutral":
                    logging.info(f"시장 상태 변경: NEUTRAL (횡보 전환)")
                self.market_mode = "neutral"
                self.oneway_strength = "none"
                self.prev_market_mode = "neutral"
                self.prev_oneway_strength = "none"

    def is_oneway_up(self):
        return self.market_mode == "oneway_up"

    def is_oneway_down(self):
        return self.market_mode == "oneway_down"

    def spread_filler_orders(self, best_bid, best_ask):
        orders = []
        spread = best_ask - best_bid
        str_mult = {"strong": 2.4, "medium": 1.5, "weak": 1.1, "none": 1.0}[self.oneway_strength]
        
        threshold = 5 if self.is_warmup_period() else SPREAD_FILLER_THRESHOLD
        is_warmup = self.is_warmup_period()
        
        if spread >= self.ticksize * threshold:
            n_orders = min(max(spread // self.ticksize - 1, 2), 14)
            
            if is_warmup:
                n_orders = min(max(spread // self.ticksize - 1, 8), 30)
            
            px_list = [best_bid + i * self.ticksize for i in range(1, n_orders)]
            
            if self.is_oneway_up():
                for px in px_list:
                    qty = int(random.randint(750, 1500) * str_mult)
                    orders.append({"side": "buy", "type": "limit", "price": px, "quantity": qty, "persistent": is_warmup})
                    sell_px = px + random.randint(2, 8) * self.ticksize
                    orders.append({"side": "sell", "type": "limit", "price": sell_px, "quantity": qty, "persistent": is_warmup})
            elif self.is_oneway_down():
                for px in px_list:
                    qty = int(random.randint(750, 1500) * str_mult)
                    orders.append({"side": "sell", "type": "limit", "price": px, "quantity": qty, "persistent": is_warmup})
                    buy_px = px - random.randint(2, 8) * self.ticksize
                    orders.append({"side": "buy", "type": "limit", "price": buy_px, "quantity": qty, "persistent": is_warmup})
            else:
                for px in px_list:
                    base_qty = random.randint(750, 1500)
                    if is_warmup:
                        base_qty = random.randint(2400, 5000)  
                    qty = int(base_qty * str_mult)
                    side = random.choice(["buy", "sell"])
                    orders.append({"side": side, "type": "limit", "price": px, "quantity": qty, "persistent": is_warmup})
        return orders

    def decide_orders(self):
        self.maybe_trigger_oneway()
        self.set_liquidity_level()
        ref_price = self.get_reference_price()
        orders = []
        
        # 웜업 기간: 두터운 호가 조성
        if self.is_warmup_period():
            n_limit = random.randint(25, 40)
            for _ in range(n_limit):
                buy_offset = random.randint(1, 60)
                buy_price = self.nearest_tick(ref_price - buy_offset * self.ticksize)
                buy_qty = random.randint(400, 800)  
                orders.append({"side": "buy", "type": "limit", "price": buy_price, "quantity": buy_qty, "persistent": True})
                
                sell_offset = random.randint(1, 60)
                sell_price = self.nearest_tick(ref_price + sell_offset * self.ticksize)
                sell_qty = random.randint(400, 800)  
                orders.append({"side": "sell", "type": "limit", "price": sell_price, "quantity": sell_qty, "persistent": True})
            
            n_market = random.randint(3, 8)
            for _ in range(n_market):
                side = random.choice(["buy", "sell"])
                market_qty = random.randint(50, 150)
                orders.append({"side": side, "type": "market", "quantity": market_qty})
            
            if self.depth_data and self.depth_data.get("bids") and self.depth_data.get("asks"):
                best_bid = self.depth_data["bids"][0][0]
                best_ask = self.depth_data["asks"][0][0]
                orders += self.spread_filler_orders(best_bid, best_ask)
            
            # [추가됨] 웜업 기간에 생성된 지정가 호가는 30분(1800초) 뒤 자동 삭제 태깅
            for o in orders:
                if o['type'] == 'limit':
                    o['cancel_after'] = 1800

            return aggregate_orders(orders)
        
        # 원웨이 장세
        if self.is_oneway_up():
            str_mult = {"strong": 2.4, "medium": 1.8, "weak": 1.5}[self.oneway_strength]
            n = int(random.randint(2, 5) * str_mult)
            orders += [{"side": "buy", "type": "limit", "price": self.nearest_tick(ref_price + int(str_mult * random.randint(10, 30)) * self.ticksize), "quantity": int(random.randint(7, 150) * str_mult)} for _ in range(n)]
            orders += [{"side": "sell", "type": "limit", "price": self.nearest_tick(ref_price + int(str_mult * random.randint(12, 35)) * self.ticksize), "quantity": int(random.randint(1, 50) * str_mult)} for _ in range(n)]
        elif self.is_oneway_down():
            str_mult = {"strong": 2.4, "medium": 1.5, "weak": 1.1}[self.oneway_strength]
            n = int(random.randint(2, 5) * str_mult)
            orders += [{"side": "sell", "type": "limit", "price": self.nearest_tick(ref_price - int(str_mult * random.randint(10, 30)) * self.ticksize), "quantity": int(random.randint(7, 150) * str_mult)} for _ in range(n)]
            orders += [{"side": "buy", "type": "limit", "price": self.nearest_tick(ref_price - int(str_mult * random.randint(12, 35)) * self.ticksize), "quantity": int(random.randint(1, 50) * str_mult)} for _ in range(n)]
        else:
            # 횡보/일반 장세
            is_whale_active = random.random() < self.whale_ratio
            if is_whale_active:
                orders += self.whale_orders(ref_price)
            else:
                # 시장 미세 추세 설정 (약상승/약하락/중립)
                market_trend = random.choices(
                    ["slight_up", "slight_down", "neutral"],
                    [UPWARD_BIAS, 1 - UPWARD_BIAS, 0.2]
                )[0]
                
                if market_trend != self.prev_market_trend:
                    trend_name = {"slight_up": "횡보:약상승", "slight_down": "횡보:약하락", "neutral": "횡보:중립"}[market_trend]
                    logging.info(f"시장 상태 변경: {trend_name}")
                    self.prev_market_trend = market_trend

                # 추세에 따라 매수/매도 수량 불균형 설정 (Skew)
                if market_trend == "slight_up":
                    buy_qty_mult = 2.5   # 매수 수량 가중치
                    sell_qty_mult = 0.4  # 매도 수량 가중치
                elif market_trend == "slight_down":
                    buy_qty_mult = 0.4
                    sell_qty_mult = 2.5
                else:
                    buy_qty_mult = 1.0
                    sell_qty_mult = 1.0

                # 호가 레이어링 (현재가 주변을 촘촘하게 채움)
                num_layers = random.randint(2, 4)
                base_qty = random.randint(20, 60)

                for i in range(1, num_layers + 1):
                    # 매도 호가
                    ask_px = self.nearest_tick(ref_price + i * self.ticksize)
                    ask_qty = int(base_qty * sell_qty_mult * random.uniform(0.8, 1.2))
                    orders.append({"side": "sell", "type": "limit", "price": ask_px, "quantity": max(ask_qty, 1)})

                    # 매수 호가
                    bid_px = self.nearest_tick(ref_price - i * self.ticksize)
                    bid_qty = int(base_qty * buy_qty_mult * random.uniform(0.8, 1.2))
                    orders.append({"side": "buy", "type": "limit", "price": bid_px, "quantity": max(bid_qty, 1)})

                # 실제 체결 유도를 위한 소량의 시장가 주문 트리거
                if random.random() < 0.3:
                    trigger_side = "buy" if market_trend == "slight_up" else ("sell" if market_trend == "slight_down" else random.choice(["buy", "sell"]))
                    orders.append({"side": trigger_side, "type": "market", "quantity": random.randint(5, 20)})
        
        # 스프레드 필러 (호가 공백 메우기)
        if self.depth_data and self.depth_data.get("bids") and self.depth_data.get("asks"):
            best_bid = self.depth_data["bids"][0][0]
            best_ask = self.depth_data["asks"][0][0]
            orders += self.spread_filler_orders(best_bid, best_ask)
        
        return aggregate_orders(orders)

    def update_depth(self, depth):
        self.depth_data = depth["depth"]

    def update_ledger(self, ledger):
        self.ledger_data = ledger
        if ledger and ledger.get("ledger"):
            self.last_trade_price = ledger["ledger"][0]["price"]

    def update_session(self, session):
        self.session_data = session.get("session")

        if self.session_data == "closed":
            if self.market_open_event.is_set():
                if self.last_trade_price is not None:
                    self.last_close_price = self.last_trade_price
                    self.save_last_price()
                    logging.info(f"시장 종료: 거래 중단 (종가: {self.last_close_price})")
                else:
                    logging.info("시장 종료: 거래 중단")
                self.market_open_event.clear()
                self.market_opened_at = None
        else:
            if not self.market_open_event.is_set():
                if self.last_close_price is not None and random.random() < GAP_PROBABILITY:
                    gap_ticks = random.randint(GAP_MIN_TICKS, GAP_MAX_TICKS)
                    gap_amount = gap_ticks * self.ticksize
                    new_price = self.nearest_tick(self.last_close_price + gap_amount)
                    new_price = max(new_price, self.min_price)
                    
                    self.fallback_price = new_price
                    self.last_trade_price = new_price
                    self.save_last_price()
                    
                    logging.info(f"시장 개장: 갭 발생 {self.last_close_price} -> {new_price}")
                else:
                    logging.info(f"시장 개장: 거래 재개")
                
                self.market_opened_at = time.time()
                self.market_open_event.set()

    async def maybe_cancel_top_counterparty(self, session):
        if random.random() >= CANCEL_TOP_PROB:
            return

        if not self.depth_data:
            return

        if self.is_oneway_up():
            side = 'sell'
            book_side = self.depth_data.get('asks', [])
        elif self.is_oneway_down():
            side = 'buy'
            book_side = self.depth_data.get('bids', [])
        else:
            return

        if not book_side:
            return

        top = book_side[0]
        order_ids = []
        if len(top) >= 3:
            third = top[2]
            if isinstance(third, list) and third:
                for it in third:
                    if isinstance(it, dict) and it.get('order_id'):
                        order_ids.append(it.get('order_id'))
                    elif isinstance(it, str):
                        order_ids.append(it)
        
        if not order_ids:
            return

        async def _cancel_worker(session, side, order_ids):
            deadline = time.time() + 1800 
            ids = list(order_ids)
            random.shuffle(ids)
            tried = set()
            for oid in ids:
                if time.time() > deadline:
                    return
                if oid in tried:
                    continue
                tried.add(oid)
                try:
                    status, result = await delete_order(session, side, oid)
                except Exception:
                    await asyncio.sleep(0.1)
                    continue

                if status == 200:
                    logging.info(f"상대방 주문 취소 성공: order_id={oid}")
                    return
                else:
                    await asyncio.sleep(0.05)
                    continue

        try:
            asyncio.create_task(_cancel_worker(session, side, order_ids))
        except Exception as e:
            logging.error(f"취소 작업 실패: {e}")

async def decode_base64_gzip(data):
    return json.loads(data)

async def delete_order(session, side, order_id):
    url = f"{ORDER_API_BASE}/{SYMBOL}/{side}"
    body = {"order_id": order_id}
    try:
        async with session.delete(url, json=body, headers=HEADERS) as resp:
            status = resp.status
            try:
                result = await resp.json()
            except Exception:
                result = None
            return status, result
    except Exception as e:
        logging.error(f"주문 취소 에러: {e}")
        return None, {"error": str(e)}

async def schedule_order_delete(session, side, order_id, delay):
    await asyncio.sleep(delay)
    await delete_order(session, side, order_id)

async def send_order(session, order):
    url = f"{ORDER_API_BASE}/{SYMBOL}/{order['side']}"
    body = {"type": order["type"], "quantity": int(order["quantity"])}
    if order["type"] == "limit":
        body["price"] = int(order["price"])
    
    if order.get("log") == "[WHALE]":
        logging.info(f"🐋 WHALE ORDER: {order['side']} {order['quantity']}")

    try:
        async with session.post(url, json=body, headers=HEADERS) as resp:
            result = await resp.json()
            # [추가됨] cancel_after 값이 있으면 해당 시간 뒤 자동 취소 예약
            if result and order.get("cancel_after"):
                order_info = result.get("order")
                if order_info and isinstance(order_info, dict):
                    inner_order = order_info.get("order")
                    if inner_order and isinstance(inner_order, dict):
                        order_id = inner_order.get("order_id")
                        side = inner_order.get("side", order["side"])
                        if order_id:
                            asyncio.create_task(schedule_order_delete(session, side, order_id, order["cancel_after"]))
            return result
    except Exception as e:
        logging.error(f"주문 전송 실패: {e}")
        return None

async def sse_listener(bot):
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                async with session.get(SSE_URL, headers=HEADERS) as resp:
                    async for line in resp.content:
                        line = line.decode().strip()
                        if line.startswith("event:"):
                            event = line.split("event:", 1)[1].strip()
                        elif line.startswith("data:"):
                            data = line.split("data:", 1)[1].strip()
                            if event == "depth":
                                bot.update_depth(await decode_base64_gzip(data))
                            elif event == "ledger":
                                bot.update_ledger(await decode_base64_gzip(data))
                            elif event == "session":
                                bot.update_session(await decode_base64_gzip(data))
            except Exception as e:
                print(f"SSE 연결 에러: {e}")
                await asyncio.sleep(2)

async def trading_loop(bot):
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            await bot.market_open_event.wait()

            orders = bot.decide_orders()
            if orders:
                tasks = [send_order(session, order) for order in orders]
                await asyncio.gather(*tasks, return_exceptions=True)
                await bot.maybe_cancel_top_counterparty(session)

            interval = bot.get_trading_interval()
            await asyncio.sleep(interval)

async def main():
    bot = UltraFastMarketBot(
        fallback_price=FALLBACK_PRICE,
        ticksize=TICKSIZE,
        whale_ratio=WHALE_RATIO
    )
    await asyncio.gather(
        sse_listener(bot),
        trading_loop(bot)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("종료됨")
