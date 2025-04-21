# smart_money_detector.py
import grpc
import logging
import os
import time
from collections import defaultdict, deque, namedtuple
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Tuple, Optional, FrozenSet

try:
    import geyser_pb2
    import geyser_pb2_grpc
except ImportError:
    print("Error: Could not import gRPC generated files.")
    exit(1)

from solders.pubkey import Pubkey
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
GRPC_ENDPOINT = os.getenv("GRPC_ENDPOINT", "zewr2.tty0.co:10001")

# Logging
LOG_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

# Buy event structure
BuyEvent = namedtuple("BuyEvent", ["timestamp", "buyer", "token_mint", "amount", "market"])

# Parameters
TIME_WINDOW_SECONDS = 30  # Time window to group events
MIN_GROUP_SIZE = 3        # Minimum addresses in a group
MIN_GROUP_SIGHTINGS = 2   # Minimum tokens bought by the same group
HISTORY_MINUTES = 10      # How long to keep events

# Data stores
recent_buys: deque[BuyEvent] = deque()
group_sightings: Dict[FrozenSet[str], List[str]] = defaultdict(list)
group_first_buyer: Dict[FrozenSet[str], List[str]] = defaultdict(list)
LEADER_LOG_COOLDOWN_SECONDS = 300
last_logged_leader: Dict[str, datetime] = {}

# Constants
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RAYDIUM_AMM_V4_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

# Helper functions
def parse_balance_changes(meta) -> Dict[str, Dict[str, Tuple[int, int]]]:
    """Parse token balance changes: {owner: {mint: (pre_amount, post_amount)}}"""
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

def find_market(account_keys: List[bytes], instructions) -> str:
    """Identify the DEX market from the transaction instructions."""
    try:
        pubkeys = [str(Pubkey(key)) for key in account_keys]
        for inst in instructions:
            if hasattr(inst, "program_id_index"):
                idx = inst.program_id_index
                if idx < len(pubkeys):
                    program_id = pubkeys[idx]
                    if program_id == PUMP_FUN_PROGRAM_ID:
                        return "PumpFun"
                    if program_id == RAYDIUM_AMM_V4_PROGRAM_ID:
                        return "Raydium"
    except Exception as e:
        logging.error(f"Error parsing market from instructions: {e}")
    return "Unknown"


def detect_buyers(balance_changes) -> List[Tuple[str, str, int]]:
    """Detect buyers from balance changes."""
    results = []
    for owner, tokens in balance_changes.items():
        for mint, (pre, post) in tokens.items():
            if post > pre and mint != WSOL_MINT:
                for quote_mint in [WSOL_MINT, USDC_MINT]:
                    if quote_mint in tokens and tokens[quote_mint][1] < tokens[quote_mint][0]:
                        amount_bought = post - pre
                        results.append((owner, mint, amount_bought))
                        break
    return results

def prune_old_events(now):
    cutoff = now - timedelta(minutes=HISTORY_MINUTES)
    while recent_buys and recent_buys[0].timestamp < cutoff:
        recent_buys.popleft()

def find_groups(now: datetime, new_event: BuyEvent):
    window_start = now - timedelta(seconds=TIME_WINDOW_SECONDS)
    matched = []

    for event in reversed(recent_buys):
        if event.timestamp < window_start:
            break
        if event.token_mint == new_event.token_mint and event.buyer != new_event.buyer:
            matched.append(event.buyer)

    if len(matched) >= MIN_GROUP_SIZE - 1:
        group = frozenset(matched + [new_event.buyer])
        group_sightings[group].append(new_event.token_mint)
        group_first_buyer[group].append(new_event.buyer)
        analyze_group(group)

def analyze_group(group: FrozenSet[str]):
    tokens_seen = set(group_sightings[group])
    if len(tokens_seen) >= MIN_GROUP_SIGHTINGS:
        buyer_count = defaultdict(int)
        for first_buyer in group_first_buyer[group]:
            buyer_count[first_buyer] += 1

        leader, votes = max(buyer_count.items(), key=lambda x: x[1])
        total_votes = sum(buyer_count.values())

        if votes > total_votes / 2:
            now = datetime.now(timezone.utc)
            last_log = last_logged_leader.get(leader)
            if not last_log or (now - last_log).total_seconds() > LEADER_LOG_COOLDOWN_SECONDS:
                print("-" * 40)
                logging.warning(f"Potential Leader: {leader}")
                logging.warning(f"Group Members: {list(group)}")
                logging.warning(f"Tokens Seen: {len(tokens_seen)}")
                logging.warning(f"Votes: {votes}/{total_votes}")
                print("-" * 40)
                last_logged_leader[leader] = now

# Main gRPC listening loop
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
                                instructions = list(message.instructions)
                                market = find_market(account_keys, instructions)
                                balance_changes = parse_balance_changes(meta)
                                buys = detect_buyers(balance_changes)

                                for buyer, mint, amount in buys:
                                    event = BuyEvent(now_utc, buyer, mint, amount, market)
                                    recent_buys.append(event)
                                    prune_old_events(now_utc)
                                    find_groups(now_utc, event)

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
