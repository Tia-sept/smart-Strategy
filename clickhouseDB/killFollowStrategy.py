from strategy import Strategy
from collections import defaultdict
import datetime

class KillFollowStrategy(Strategy):
    def __init__(self, client, max_hold_time_sec=30):
        self.client = client
        self.max_hold_time_sec = max_hold_time_sec

    def detect_leader(self):
        query = f"""
        SELECT
            ts, mint, traderPublicKey, txType, marketCapSol
        FROM solana.pumpfun_from_events_mv
        WHERE ts >= now() - INTERVAL 1 DAY
        AND txType IN ('buy', 'sell')
        ORDER BY traderPublicKey, mint, ts
        """
        rows = self.client.execute(query)

        suspicious = defaultdict(list)
        trades = defaultdict(lambda: defaultdict(list))
        
        for ts, mint, trader, tx_type, cap in rows:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
            trades[trader][mint].append((ts, tx_type))

        for trader, mint_dict in trades.items():
            for mint, actions in mint_dict.items():
                actions.sort() 
                for i, (ts_i, type_i) in enumerate(actions):
                    if type_i != 'buy':
                        continue
                    for j in range(i + 1, len(actions)):
                        ts_j, type_j = actions[j]
                        if type_j != 'sell':
                            continue
                        delta = (ts_j - ts_i).total_seconds()
                        if 0 < delta <= self.max_hold_time_sec:
                            suspicious[trader].append(mint)
                            break  

        return {addr: list(set(tokens)) for addr, tokens in suspicious.items() if len(set(tokens)) >= 2}
