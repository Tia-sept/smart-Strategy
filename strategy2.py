# low_cap_fast_trader_detector.py

import grpc
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

try:
    import geyser_pb2
    import geyser_pb2_grpc
except ImportError:
    print("Error: Could not import gRPC generated files.")
    exit(1)

from solders.pubkey import Pubkey
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()
GRPC_ENDPOINT = os.getenv("GRPC_ENDPOINT", "zewr2.tty0.co:10001")

# Logging
LOG_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

# Buy/Sell event
BuySellEvent = Tuple[datetime, str, str, int, bool]  # (timestamp, buyer, mint, amount, is_sell)

# Parameters
TIME_WINDOW_SECONDS = 30
HISTORY_MINUTES = 10
MARKET_CAP_THRESHOLD = 500_000  # 市值小于50万美金的token才分析

# Data store
recent_events: deque[BuySellEvent] = deque()
buyer_histories: Dict[str, List[BuySellEvent]] = defaultdict(list)
token_market_caps: Dict[str, float] = {}

# Constants
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RAYDIUM_AMM_V4_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

def fetch_market_cap(mint: str) -> float:
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=5)

        if resp.status_code != 200:
            logging.error(f"Failed to fetch market cap for {mint}: HTTP {resp.status_code}")
            return 0

        data = resp.json()

        pairs = data.get('pairs', [])
        if not pairs:
            logging.warning(f"No trading pairs found for {mint} on Dexscreener.")
            return 0

        fdv = pairs[0].get('fdv')
        if fdv is None:
            logging.warning(f"FDV not available for {mint} in the first pair.")
            return 0

        return float(fdv)

    except requests.RequestException as e:
        logging.error(f"Network error fetching market cap for {mint}: {e}")
        return 0
    except ValueError as e:
        logging.error(f"JSON parse error fetching market cap for {mint}: {e}")
        return 0
    except Exception as e:
        logging.error(f"Unexpected error fetching market cap for {mint}: {e}")
        return 0
def parse_balance_changes(meta) -> Dict[str, Dict[str, Tuple[int, int]]]:
    changes = defaultdict(lambda: defaultdict(lambda: (0, 0)))
    for b in getattr(meta, "pre_token_balances", []):
        owner = str(b.owner)
        mint = str(b.mint)
        amount = int(getattr(getattr(b, "ui_token_amount", None), "amount", "0"))
        changes[owner][mint] = (amount, 0)
    for b in getattr(meta, "post_token_balances", []):
        owner = str(b.owner)
        mint = str(b.mint)
        post_amount = int(getattr(getattr(b, "ui_token_amount", None), "amount", "0"))
        pre_amount = changes[owner][mint][0]
        changes[owner][mint] = (pre_amount, post_amount)
    return changes

def detect_buys_sells(balance_changes) -> List[Tuple[str, str, int, bool]]:
    results = []
    for owner, tokens in balance_changes.items():
        for mint, (pre, post) in tokens.items():
            if mint == WSOL_MINT:
                continue
            if post > pre:
                results.append((owner, mint, post - pre, False))  # Buy
            elif pre > post:
                results.append((owner, mint, pre - post, True))   # Sell
    return results

def prune_old_events(now):
    cutoff = now - timedelta(minutes=HISTORY_MINUTES)
    while recent_events and recent_events[0][0] < cutoff:
        recent_events.popleft()

def analyze_fast_traders(now: datetime, event: BuySellEvent):
    buyer, mint = event[1], event[2]
    history = buyer_histories[buyer]
    relevant_events = [e for e in history if e[2] == mint]

    if len(relevant_events) < 2:
        return

    for past_event in relevant_events:
        if past_event[4] == False and event[4] == True:
            time_diff = (event[0] - past_event[0]).total_seconds()
            if 0 < time_diff <= TIME_WINDOW_SECONDS:
                # 买入后30秒内卖出
                cap = token_market_caps.get(mint, 0)
                if cap == 0:
                    cap = fetch_market_cap(mint)
                    token_market_caps[mint] = cap

                if cap <= MARKET_CAP_THRESHOLD:
                    logging.warning(f"[Fast Sell Detected] {buyer} bought and sold {mint} within {time_diff:.1f}s (Market Cap: ${cap:.1f})")
                    logging.warning(f"Likely Leader Address: {buyer}")
                    print("-" * 40)
                break

# Main loop
def run():
    channel_options = [
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.keepalive_permit_without_calls", True),
        ("grpc.http2.min_time_between_pings_ms", 15000),
        ("grpc.http2.min_ping_interval_without_data_ms", 10000),
        ("grpc.max_receive_message_length", -1)
    ]

    with grpc.insecure_channel(GRPC_ENDPOINT, options=channel_options) as channel:
        stub = geyser_pb2_grpc.GeyserStub(channel)
        subscribe_request = geyser_pb2.SubscribeRequest(
            transactions={
                "filter": geyser_pb2.SubscribeRequestFilterTransactions(
                    vote=False,
                    failed=False,
                    account_include=[PUMP_FUN_PROGRAM_ID, RAYDIUM_AMM_V4_PROGRAM_ID]
                )
            },
            commitment=geyser_pb2.CommitmentLevel.FINALIZED
        )

        while True:
            try:
                stream = stub.Subscribe(iter([subscribe_request]))
                for update in stream:
                    if update.HasField("transaction"):
                        wrapper = update.transaction
                        inner_tx = wrapper.transaction
                        meta = getattr(inner_tx, "meta", None)
                        now_utc = datetime.now(timezone.utc)

                        if meta and hasattr(inner_tx, "transaction"):
                            tx_body = inner_tx.transaction
                            if hasattr(tx_body, "message"):
                                message = tx_body.message
                                account_keys = list(message.account_keys)

                                balance_changes = parse_balance_changes(meta)
                                buys_sells = detect_buys_sells(balance_changes)

                                for buyer, mint, amount, is_sell in buys_sells:
                                    event = (now_utc, buyer, mint, amount, is_sell)
                                    recent_events.append(event)
                                    prune_old_events(now_utc)
                                    buyer_histories[buyer].append(event)
                                    if is_sell:
                                        analyze_fast_traders(now_utc, event)

            except grpc.RpcError as e:
                print(f"gRPC Error: {e.code()} - {e.details()}. Reconnecting...")
                time.sleep(5)
            except KeyboardInterrupt:
                print("Interrupted. Exiting...")
                break
            except Exception as e:
                print(f"Unexpected error: {e}. Reconnecting...")
                time.sleep(5)

if __name__ == "__main__":
    run()
