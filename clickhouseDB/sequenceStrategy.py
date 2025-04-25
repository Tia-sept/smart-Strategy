from clickhouse_driver import Client
from collections import defaultdict, Counter
from itertools import combinations
from strategy import Strategy
import datetime

class SequenceStrategy(Strategy):
    def __init__(self, client):
        self.client = client

    def fetch_buy_events(self):
        query = """
        SELECT
            ts, mint, traderPublicKey, tokenAmount
        FROM solana.pumpfun_from_events_mv
        WHERE txType = 'buy' AND ts >= now() - INTERVAL 1 DAY

        UNION ALL

        SELECT
            ts, inputTokenMint AS mint, traderPublicKey, qtyIn AS tokenAmount
        FROM solana.raydium_from_events_mv
        WHERE txType = 'buy' AND ts >= now() - INTERVAL 1 DAY
        """
        return self.client.execute(query)

    def find_clusters(self, rows, time_window_sec=10):
        mint_clusters = defaultdict(list)

        for ts, mint, trader, amount in rows:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
            mint_clusters[mint].append((ts, trader, amount))

        combo_tracker = defaultdict(list)

        for mint, events in mint_clusters.items():
            events.sort()
            for i in range(len(events)):
                ts1, t1, a1 = events[i]
                cluster = [(ts1, t1, a1)]
                for j in range(i + 1, len(events)):
                    ts2, t2, a2 = events[j]
                    if (ts2 - ts1).total_seconds() <= time_window_sec:
                        if a1 != a2:
                            cluster.append((ts2, t2, a2))
                    else:
                        break
                if len(cluster) >= 3:
                    traders = tuple(sorted([x[1] for x in cluster]))
                    combo_tracker[traders].append((mint, cluster))

        return combo_tracker

    def detect_following_patterns(self, combo_tracker):
        leader_candidates = defaultdict(list)

        for combo, mint_clusters in combo_tracker.items():
            if len(mint_clusters) < 2:
                continue
            seqs = [tuple(x[1][i][1] for i in range(len(x[1]))) for x in mint_clusters]
            if len(set(seqs)) > 1:
                firsts = [seq[0] for seq in seqs]
                common_first = Counter(firsts).most_common(1)[0][0]
                leader_candidates[common_first].append({
                    'mints': [x[0] for x in mint_clusters],
                    'participants': combo,
                    'sequences': seqs
                })

        return leader_candidates

    def detect_leader(self):
        rows = self.fetch_buy_events()
        combos = self.find_clusters(rows)
        leaders = self.detect_following_patterns(combos)
        return leaders