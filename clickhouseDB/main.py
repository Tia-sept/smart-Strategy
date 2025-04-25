from sequenceStrategy import SequenceStrategy
from killFollowStrategy import KillFollowStrategy
from clickhouse_driver import Client
import argparse
import yaml

STRATEGY_REGISTRY = {
    "KillFollowStrategy": KillFollowStrategy,
    "SequenceStrategy": SequenceStrategy
}

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)
def main():
    parser = argparse.ArgumentParser(description="Leader Detection Strategy Runner")
    parser.add_argument(
        "-c", "--config",
        type=str,
        default=None,
        help="Path to YAML config file"
    )
    args = parser.parse_args()
    if args.config:
        print(f"Loading config from {args.config}")
        config = load_config(args.config)
        
    strategy_name = config["strategy"]
    strategy_params = config.get("params", {})
    clickhouse_config = config.get("clickhouse", {})
    client = Client(
        host=clickhouse_config.get("host", "localhost"),
        port=clickhouse_config.get("port", ""),
        database=clickhouse_config.get("database", "default")
    )
    strategy_cls = STRATEGY_REGISTRY[strategy_name]
    strategy = strategy_cls(client, **strategy_params)
    
    leaders = strategy.detect_leader()
    for leader, _ in leaders.items():
        print(f"potential leader: {leader}")


if __name__ == "__main__":
    main()
