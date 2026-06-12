import os
import sys

from loguru import logger

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.clickhouse_client import ClickHouseClient


def main(argv: list[str] | None = None) -> None:
    client = ClickHouseClient.from_env()
    client.ensure_schema()
    logger.success("ClickHouse 连接检查: {}", "OK" if client.ping() else "FAILED")


if __name__ == "__main__":
    main()
