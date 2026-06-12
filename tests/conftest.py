import glob
import os
import shutil
import subprocess
import sys
import tempfile

import pytest
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

load_dotenv()


# ---------------------------------------------------------------------------
# PostgreSQL 集成测试基建
#
# 解析顺序：
# 1. TEST_DATABASE_URL 环境变量（必须指向可丢弃的测试库：会 create_all + TRUNCATE）；
# 2. 本机 postgres 二进制（Homebrew/Linux 常见路径或 PATH）：在临时目录 initdb
#    一个一次性集群，仅监听 unix socket，session 结束即销毁；
# 3. 都不可用时 skip 整个集成测试集，不影响纯单元测试。
# ---------------------------------------------------------------------------

def _require_test_database(url: str) -> None:
    """保险丝：集成测试会对目标库 create_all + TRUNCATE 全表。

    强制要求库名含 'test'，防止 TEST_DATABASE_URL 被误填成生产库。
    """
    from sqlalchemy.engine.url import make_url

    try:
        database = make_url(url).database or ""
    except Exception as exc:
        raise RuntimeError(f"TEST_DATABASE_URL 无法解析: {exc}") from exc
    if "test" not in database.lower():
        raise RuntimeError(
            f"拒绝运行：TEST_DATABASE_URL 指向库 '{database}'，库名必须包含 'test'。"
            "集成测试会 TRUNCATE 全部表——绝不允许指向生产库。"
        )


def _find_pg_bindir() -> str | None:
    candidates: list[str] = []
    for pattern in (
        "/opt/homebrew/opt/postgresql@*/bin",
        "/usr/local/opt/postgresql@*/bin",
        "/usr/lib/postgresql/*/bin",
        "/usr/pgsql-*/bin",
    ):
        candidates.extend(sorted(glob.glob(pattern), reverse=True))
    initdb_on_path = shutil.which("initdb")
    if initdb_on_path:
        candidates.append(os.path.dirname(initdb_on_path))
    for bindir in candidates:
        if all(os.path.exists(os.path.join(bindir, name)) for name in ("initdb", "pg_ctl", "postgres")):
            return bindir
    return None


@pytest.fixture(scope="session")
def pg_url():
    env_url = os.getenv("TEST_DATABASE_URL")
    if env_url:
        _require_test_database(env_url)
        yield env_url
        return

    bindir = _find_pg_bindir()
    if bindir is None:
        pytest.skip("PostgreSQL 集成测试不可用：未设置 TEST_DATABASE_URL 且找不到本地 postgres 二进制")

    # socket 路径有 ~104 字符上限，固定放在 /tmp 下
    base_dir = tempfile.mkdtemp(prefix="stock_pgtest_", dir="/tmp")
    data_dir = os.path.join(base_dir, "data")
    try:
        subprocess.run(
            [os.path.join(bindir, "initdb"), "-D", data_dir, "-U", "postgres", "--no-sync", "-E", "UTF8"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                os.path.join(bindir, "pg_ctl"),
                "-D", data_dir,
                "-o", f"-c listen_addresses='' -k {base_dir} -F",
                "-w", "-t", "30",
                "-l", os.path.join(base_dir, "server.log"),
                "start",
            ],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        shutil.rmtree(base_dir, ignore_errors=True)
        pytest.skip(f"临时 PostgreSQL 启动失败: {exc.stderr.decode(errors='replace')[:500]}")

    try:
        yield f"postgresql+psycopg2://postgres@/postgres?host={base_dir}"
    finally:
        subprocess.run(
            [os.path.join(bindir, "pg_ctl"), "-D", data_dir, "-m", "immediate", "stop"],
            capture_output=True,
        )
        shutil.rmtree(base_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def pg_engine(pg_url):
    from sqlalchemy import create_engine

    from data_models.models import Base

    engine = create_engine(pg_url)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def pg_db(pg_engine, pg_url):
    """函数级 DatabaseManager：每个用例前 TRUNCATE 全部表，保证用例独立。"""
    from sqlalchemy import text

    from data_models.models import Base
    from db_manager import DatabaseManager

    with pg_engine.connect() as conn:
        tables = ", ".join(table.name for table in Base.metadata.sorted_tables)
        conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
        conn.commit()

    manager = DatabaseManager(pg_url)
    yield manager
    manager.close()
