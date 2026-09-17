"""
RM Invest take-home: restore Polymarket wallet history via Polygon RPC → DB.

Wallet: 0x46b353667fd7d846af3bbeda6584b0e5b883d3de
Only eth_getLogs + eth_call (no Polymarket API / third-party indexers).

Default RPC: Tenderly public (archive logs). Override with POLYGON_RPC_URL.
PostgreSQL via DATABASE_URL, else SQLite fallback.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from eth_abi import decode
from web3 import AsyncWeb3, Web3
from web3.providers import AsyncHTTPProvider

try:
    import asyncpg
except ImportError:  # pragma: no cover
    asyncpg = None

WALLET = Web3.to_checksum_address("0x46b353667fd7d846af3bbeda6584b0e5b883d3de")
RPC_URL = os.getenv("POLYGON_RPC_URL", "https://gateway.tenderly.co/public/polygon")
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://polymarket:polymarket@127.0.0.1:5433/polymarket",
)
SQLITE_PATH = Path(__file__).resolve().parent / "wallet_history.db"
USE_SQLITE = os.getenv("USE_SQLITE", "0") == "1"

CTF = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
USDC_E = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
USDC_NATIVE = Web3.to_checksum_address("0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359")
PUSD = Web3.to_checksum_address("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB")

ERC20_TOKENS = {
    USDC_E: ("USDC.e", 6),
    USDC_NATIVE: ("USDC", 6),
    PUSD: ("pUSD", 6),
}

TOPIC_ERC20_TRANSFER = Web3.keccak(text="Transfer(address,address,uint256)")
TOPIC_ERC1155_SINGLE = Web3.keccak(
    text="TransferSingle(address,address,address,uint256,uint256)"
)
TOPIC_ERC1155_BATCH = Web3.keccak(
    text="TransferBatch(address,address,address,uint256[],uint256[])"
)

ERC20_ABI = [
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    }
]
ERC1155_ABI = [
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "balanceOfBatch",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "accounts", "type": "address[]"},
            {"name": "ids", "type": "uint256[]"},
        ],
        "outputs": [{"name": "", "type": "uint256[]"}],
    },
]

CHUNK = int(os.getenv("LOG_CHUNK", "9000"))
SEM = asyncio.Semaphore(int(os.getenv("RPC_CONCURRENCY", "4")))
VERIFY_BATCH = int(os.getenv("VERIFY_BATCH", "100"))
VERIFY_ONLY = os.getenv("VERIFY_ONLY", "0") == "1"
FORCE_FULL = os.getenv("FORCE_FULL", "0") == "1"


@dataclass
class TransferEvent:
    block_number: int
    log_index: int
    tx_hash: str
    token: str
    token_id: str
    from_addr: str
    to_addr: str
    amount: int
    kind: str


def topic_addr(addr: str) -> str:
    return "0x" + "0" * 24 + addr.lower().replace("0x", "")


def _data_bytes(data) -> bytes:
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if isinstance(data, str):
        return bytes.fromhex(data[2:] if data.startswith("0x") else data)
    return bytes(data)


def parse_erc20(log) -> TransferEvent | None:
    frm = Web3.to_checksum_address("0x" + log["topics"][1].hex()[-40:])
    to = Web3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])
    if frm.lower() != WALLET.lower() and to.lower() != WALLET.lower():
        return None
    amount = int.from_bytes(_data_bytes(log["data"]), "big")
    return TransferEvent(
        int(log["blockNumber"]),
        int(log["logIndex"]),
        log["transactionHash"].hex(),
        Web3.to_checksum_address(log["address"]),
        "",
        frm,
        to,
        amount,
        "erc20",
    )


def parse_erc1155_single(log) -> TransferEvent | None:
    frm = Web3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])
    to = Web3.to_checksum_address("0x" + log["topics"][3].hex()[-40:])
    if frm.lower() != WALLET.lower() and to.lower() != WALLET.lower():
        return None
    token_id, amount = decode(["uint256", "uint256"], _data_bytes(log["data"]))
    return TransferEvent(
        int(log["blockNumber"]),
        int(log["logIndex"]),
        log["transactionHash"].hex(),
        Web3.to_checksum_address(log["address"]),
        str(token_id),
        frm,
        to,
        int(amount),
        "erc1155",
    )


def parse_erc1155_batch(log) -> list[TransferEvent]:
    frm = Web3.to_checksum_address("0x" + log["topics"][2].hex()[-40:])
    to = Web3.to_checksum_address("0x" + log["topics"][3].hex()[-40:])
    if frm.lower() != WALLET.lower() and to.lower() != WALLET.lower():
        return []
    ids, values = decode(["uint256[]", "uint256[]"], _data_bytes(log["data"]))
    return [
        TransferEvent(
            int(log["blockNumber"]),
            int(log["logIndex"]),
            log["transactionHash"].hex(),
            Web3.to_checksum_address(log["address"]),
            str(tid),
            frm,
            to,
            int(amt),
            "erc1155",
        )
        for tid, amt in zip(ids, values)
    ]


async def get_logs(
    w3: AsyncWeb3,
    *,
    address: str | list[str],
    topics: list[Any],
    from_block: int,
    to_block: int,
) -> list[Any]:
    async with SEM:
        for attempt in range(8):
            try:
                return await w3.eth.get_logs(
                    {
                        "fromBlock": from_block,
                        "toBlock": to_block,
                        "address": address,
                        "topics": topics,
                    }
                )
            except Exception as e:  # noqa: BLE001
                msg = str(e).lower()
                if from_block < to_block and (
                    "block range" in msg
                    or "exceed maximum" in msg
                    or "too many" in msg
                    or "query returned more" in msg
                    or "response size" in msg
                ):
                    mid = (from_block + to_block) // 2
                    a = await get_logs(
                        w3,
                        address=address,
                        topics=topics,
                        from_block=from_block,
                        to_block=mid,
                    )
                    b = await get_logs(
                        w3,
                        address=address,
                        topics=topics,
                        from_block=mid + 1,
                        to_block=to_block,
                    )
                    return a + b
                await asyncio.sleep(0.3 * (attempt + 1))
        raise RuntimeError(f"get_logs failed {from_block}-{to_block}")


class Store:
    async def setup(self) -> None: ...
    async def insert_many(self, events: Iterable[TransferEvent]) -> int: ...
    async def set_state(self, key: str, value: str) -> None: ...
    async def get_state(self, key: str) -> str | None: ...
    async def load_all(self) -> list[dict]: ...
    async def aggregate_balances(self) -> tuple[int, dict[tuple[str, str], int]]: ...
    async def save_checks(self, rows: list[tuple]) -> None: ...
    async def close(self) -> None: ...


class SqliteStore(Store):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn: sqlite3.Connection | None = None

    async def setup(self) -> None:
        self.conn = sqlite3.connect(self.path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transfers (
              block_number INTEGER NOT NULL,
              log_index INTEGER NOT NULL,
              tx_hash TEXT NOT NULL,
              token TEXT NOT NULL,
              token_id TEXT NOT NULL DEFAULT '',
              from_addr TEXT NOT NULL,
              to_addr TEXT NOT NULL,
              amount TEXT NOT NULL,
              kind TEXT NOT NULL,
              UNIQUE(tx_hash, log_index, token_id)
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS balance_check(
              token TEXT NOT NULL,
              token_id TEXT NOT NULL DEFAULT '',
              computed TEXT NOT NULL,
              onchain TEXT NOT NULL,
              matched INTEGER NOT NULL,
              PRIMARY KEY(token, token_id)
            )
            """
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS sync_state(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self.conn.commit()

    async def insert_many(self, events: Iterable[TransferEvent]) -> int:
        rows = [
            (
                e.block_number,
                e.log_index,
                e.tx_hash,
                e.token,
                e.token_id,
                e.from_addr,
                e.to_addr,
                str(e.amount),
                e.kind,
            )
            for e in events
        ]
        if not rows:
            return 0
        assert self.conn
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO transfers
            (block_number,log_index,tx_hash,token,token_id,from_addr,to_addr,amount,kind)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        self.conn.commit()
        return len(rows)

    async def set_state(self, key: str, value: str) -> None:
        assert self.conn
        self.conn.execute(
            "INSERT OR REPLACE INTO sync_state(key,value) VALUES(?,?)", (key, value)
        )
        self.conn.commit()

    async def get_state(self, key: str) -> str | None:
        assert self.conn
        row = self.conn.execute(
            "SELECT value FROM sync_state WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else None

    async def load_all(self) -> list[dict]:
        assert self.conn
        cur = self.conn.execute(
            "SELECT token,token_id,from_addr,to_addr,amount FROM transfers"
        )
        cols = ["token", "token_id", "from_addr", "to_addr", "amount"]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    async def aggregate_balances(self) -> tuple[int, dict[tuple[str, str], int]]:
        rows = await self.load_all()
        balances: dict[tuple[str, str], int] = defaultdict(int)
        for r in rows:
            key = (r["token"], r["token_id"] or "")
            amt = int(r["amount"])
            if r["to_addr"].lower() == WALLET.lower():
                balances[key] += amt
            if r["from_addr"].lower() == WALLET.lower():
                balances[key] -= amt
        return len(rows), balances

    async def save_checks(self, rows: list[tuple]) -> None:
        assert self.conn
        self.conn.execute("DELETE FROM balance_check")
        self.conn.executemany(
            "INSERT OR REPLACE INTO balance_check(token,token_id,computed,onchain,matched) VALUES(?,?,?,?,?)",
            [(a, b, c, d, int(e)) for a, b, c, d, e in rows],
        )
        self.conn.commit()

    async def close(self) -> None:
        if self.conn:
            self.conn.close()


class PgStore(Store):
    def __init__(self, url: str) -> None:
        self.url = url
        self.pool = None

    async def setup(self) -> None:
        self.pool = await asyncpg.create_pool(self.url, min_size=1, max_size=4)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS transfers (
                  block_number BIGINT NOT NULL,
                  log_index INT NOT NULL,
                  tx_hash TEXT NOT NULL,
                  token TEXT NOT NULL,
                  token_id TEXT NOT NULL DEFAULT '',
                  from_addr TEXT NOT NULL,
                  to_addr TEXT NOT NULL,
                  amount NUMERIC(78,0) NOT NULL,
                  kind TEXT NOT NULL,
                  UNIQUE(tx_hash, log_index, token_id)
                );
                CREATE TABLE IF NOT EXISTS sync_state(
                  key TEXT PRIMARY KEY, value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS balance_check(
                  token TEXT NOT NULL,
                  token_id TEXT NOT NULL DEFAULT '',
                  computed NUMERIC(78,0) NOT NULL,
                  onchain NUMERIC(78,0) NOT NULL,
                  matched BOOLEAN NOT NULL,
                  checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  PRIMARY KEY(token, token_id)
                );
                """
            )

    async def insert_many(self, events: Iterable[TransferEvent]) -> int:
        rows = [
            (
                e.block_number,
                e.log_index,
                e.tx_hash,
                e.token,
                e.token_id,
                e.from_addr,
                e.to_addr,
                str(e.amount),
                e.kind,
            )
            for e in events
        ]
        if not rows:
            return 0
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO transfers
                (block_number,log_index,tx_hash,token,token_id,from_addr,to_addr,amount,kind)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT(tx_hash,log_index,token_id) DO NOTHING
                """,
                rows,
            )
        return len(rows)

    async def set_state(self, key: str, value: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sync_state(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
                key,
                value,
            )

    async def get_state(self, key: str) -> str | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT value FROM sync_state WHERE key=$1", key
            )

    async def load_all(self) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT token,token_id,from_addr,to_addr,amount FROM transfers"
            )
            return [dict(r) for r in rows]

    async def aggregate_balances(self) -> tuple[int, dict[tuple[str, str], int]]:
        async with self.pool.acquire() as conn:
            n = int(await conn.fetchval("SELECT COUNT(*) FROM transfers"))
            rows = await conn.fetch(
                """
                SELECT token, COALESCE(token_id,'') AS token_id,
                  SUM(CASE WHEN LOWER(to_addr)=$1 THEN amount ELSE 0 END)
                - SUM(CASE WHEN LOWER(from_addr)=$1 THEN amount ELSE 0 END) AS bal
                FROM transfers
                GROUP BY token, COALESCE(token_id,'')
                """,
                WALLET.lower(),
            )
            balances = {
                (r["token"], r["token_id"] or ""): int(r["bal"]) for r in rows
            }
            return n, balances

    async def save_checks(self, rows: list[tuple]) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM balance_check")
            await conn.executemany(
                "INSERT INTO balance_check(token,token_id,computed,onchain,matched) VALUES($1,$2,$3,$4,$5)",
                rows,
            )

    async def close(self) -> None:
        await self.pool.close()


async def stream_query(
    w3: AsyncWeb3,
    store: Store,
    *,
    address: str | list[str],
    topics: list[Any],
    from_block: int,
    to_block: int,
    label: str,
    parser,
) -> int:
    total = 0
    start = from_block
    while start <= to_block:
        end = min(start + CHUNK - 1, to_block)
        logs = await get_logs(
            w3, address=address, topics=topics, from_block=start, to_block=end
        )
        events: list[TransferEvent] = []
        for log in logs:
            parsed = parser(log)
            if parsed is None:
                continue
            if isinstance(parsed, list):
                events.extend(parsed)
            else:
                events.append(parsed)
        n = await store.insert_many(events)
        total += n
        print(f"  [{label}] {start}-{end} logs={len(logs)} saved~={n} total_saved~={total}")
        start = end + 1
    return total


async def resolve_block_range(w3: AsyncWeb3, store: Store) -> tuple[int, int] | None:
    """Return (start, tip) to index, or None if indexing should be skipped."""
    tip = await w3.eth.block_number
    if VERIFY_ONLY:
        print(f"VERIFY_ONLY=1 - skip indexing (chain tip={tip})")
        return None

    stored_tip = await store.get_state("tip_block")
    env_start = os.getenv("START_BLOCK")

    if env_start is not None:
        start = int(env_start)
        print(f"START_BLOCK={start} (explicit)")
    elif stored_tip and not FORCE_FULL:
        start = int(stored_tip) + 1
        print(f"Catch-up from tip_block={stored_tip} -> {start}..{tip}")
    elif FORCE_FULL:
        start = 0
        print("FORCE_FULL=1 - reindex from 0")
    else:
        start = max(0, tip - 3_000_000)
        print(f"Fresh run default window tip-3M -> {start}..{tip}")

    if start > tip:
        print(f"Already synced through {stored_tip} (tip={tip})")
        return None
    return start, tip


async def index_wallet(w3: AsyncWeb3, store: Store) -> None:
    rng = await resolve_block_range(w3, store)
    if rng is None:
        return
    start, tip = rng
    wallet_topic = topic_addr(WALLET)
    print(f"Indexing {WALLET} blocks {start}..{tip} via {RPC_URL}")

    # sequential streams keep RPC + DB calm; still correct
    await stream_query(
        w3,
        store,
        address=list(ERC20_TOKENS.keys()),
        topics=[TOPIC_ERC20_TRANSFER, wallet_topic, None],
        from_block=start,
        to_block=tip,
        label="erc20-from",
        parser=parse_erc20,
    )
    await stream_query(
        w3,
        store,
        address=list(ERC20_TOKENS.keys()),
        topics=[TOPIC_ERC20_TRANSFER, None, wallet_topic],
        from_block=start,
        to_block=tip,
        label="erc20-to",
        parser=parse_erc20,
    )
    await stream_query(
        w3,
        store,
        address=CTF,
        topics=[TOPIC_ERC1155_SINGLE, None, wallet_topic, None],
        from_block=start,
        to_block=tip,
        label="1155s-from",
        parser=parse_erc1155_single,
    )
    await stream_query(
        w3,
        store,
        address=CTF,
        topics=[TOPIC_ERC1155_SINGLE, None, None, wallet_topic],
        from_block=start,
        to_block=tip,
        label="1155s-to",
        parser=parse_erc1155_single,
    )
    await stream_query(
        w3,
        store,
        address=CTF,
        topics=[TOPIC_ERC1155_BATCH, None, wallet_topic, None],
        from_block=start,
        to_block=tip,
        label="1155b-from",
        parser=parse_erc1155_batch,
    )
    await stream_query(
        w3,
        store,
        address=CTF,
        topics=[TOPIC_ERC1155_BATCH, None, None, wallet_topic],
        from_block=start,
        to_block=tip,
        label="1155b-to",
        parser=parse_erc1155_batch,
    )
    await store.set_state("tip_block", str(tip))
    await store.set_state("wallet", WALLET)
    prev_start = await store.get_state("start_block")
    if not prev_start or int(prev_start) > start:
        await store.set_state("start_block", str(start))


async def _rpc_call(coro_factory, retries: int = 6):
    last = None
    for attempt in range(retries):
        try:
            async with SEM:
                return await coro_factory()
        except Exception as e:  # noqa: BLE001
            last = e
            await asyncio.sleep(0.4 * (attempt + 1))
    raise RuntimeError(f"RPC call failed after retries: {last}")


async def verify(w3: AsyncWeb3, store: Store) -> bool:
    n_transfers, balances = await store.aggregate_balances()
    print(f"\nTransfers loaded: {n_transfers} (unique keys={len(balances)})")
    print("=== Computed vs on-chain ===")
    all_ok = True
    checks: list[tuple] = []

    for token, (symbol, decimals) in ERC20_TOKENS.items():
        computed = balances.get((token, ""), 0)
        # token keys may differ by checksum casing
        if (token, "") not in balances:
            for (t, tid), bal in balances.items():
                if tid == "" and t.lower() == token.lower():
                    computed = bal
                    break
        contract = w3.eth.contract(address=token, abi=ERC20_ABI)
        onchain = int(
            await _rpc_call(lambda c=contract: c.functions.balanceOf(WALLET).call())
        )
        matched = computed == onchain
        all_ok &= matched
        print(
            f"{symbol}: computed={computed/(10**decimals):.6f} "
            f"onchain={onchain/(10**decimals):.6f} OK={matched}"
            + (f" delta={(computed-onchain)/(10**decimals):.6f}" if not matched else "")
        )
        checks.append((token, "", str(computed), str(onchain), matched))

    ctf = w3.eth.contract(address=CTF, abi=ERC1155_ABI)
    keys = [
        (t, tid, bal)
        for (t, tid), bal in balances.items()
        if t.lower() == CTF.lower() and tid
    ]
    # Only nonzero computed balances need on-chain proof for current holdings;
    # zero/zero would match and costs ~400k RPC calls otherwise.
    to_check = [(t, tid, bal) for t, tid, bal in keys if bal != 0]
    skipped_zero = len(keys) - len(to_check)
    bad = 0
    checked = 0
    print(
        f"CTF: ids_total={len(keys)} nonzero_computed={len(to_check)} "
        f"skip_zero={skipped_zero} batch={VERIFY_BATCH}"
    )

    async def check_batch(batch: list[tuple[str, str, int]]) -> list[tuple]:
        ids = [int(tid) for _, tid, _ in batch]
        accounts = [WALLET] * len(ids)
        try:
            onchain_list = await _rpc_call(
                lambda: ctf.functions.balanceOfBatch(accounts, ids).call()
            )
            return [
                (token, tid, computed, int(onchain), computed == int(onchain))
                for (token, tid, computed), onchain in zip(batch, onchain_list)
            ]
        except Exception as e:  # noqa: BLE001
            print(f"  batch failed ({e}); fallback single calls")
            out = []
            for token, tid, computed in batch:
                onchain = int(
                    await _rpc_call(
                        lambda tid=tid: ctf.functions.balanceOf(
                            WALLET, int(tid)
                        ).call()
                    )
                )
                out.append((token, tid, computed, onchain, computed == onchain))
            return out

    batches = [
        to_check[i : i + VERIFY_BATCH]
        for i in range(0, len(to_check), VERIFY_BATCH)
    ]
    parallel = max(1, int(os.getenv("RPC_CONCURRENCY", "4")))
    for i in range(0, len(batches), parallel):
        wave = batches[i : i + parallel]
        results_lists = await asyncio.gather(*[check_batch(b) for b in wave])
        for results in results_lists:
            for token, tid, computed, onchain, matched in results:
                checked += 1
                if not matched:
                    bad += 1
                    all_ok = False
                    if bad <= 30:
                        print(
                            f"CTF id={tid}: computed={computed} "
                            f"onchain={onchain} MISMATCH"
                        )
                checks.append((token, tid, str(computed), str(onchain), matched))
        print(
            f"  CTF verified {checked}/{len(to_check)} "
            f"mismatches={bad} waves={i // parallel + 1}"
        )

    print(f"CTF ids tracked={len(keys)} checked={checked} mismatches={bad}")
    await store.save_checks(checks)
    print("RESULT:", "ALL MATCH" if all_ok else "MISMATCH")
    stored_start = await store.get_state("start_block")
    if stored_start and int(stored_start) > 0:
        print(
            "NOTE: partial history if start_block>0 - balances match only if no earlier transfers."
        )
    return all_ok


async def open_store() -> Store:
    if USE_SQLITE or asyncpg is None:
        print(f"Using SQLite: {SQLITE_PATH}")
        return SqliteStore(SQLITE_PATH)
    try:
        store = PgStore(DATABASE_URL)
        await store.setup()
        print("Using PostgreSQL")
        return store
    except Exception as e:  # noqa: BLE001
        print(f"PostgreSQL unavailable ({e}); SQLite fallback")
        store = SqliteStore(SQLITE_PATH)
        await store.setup()
        return store


async def main() -> None:
    w3 = AsyncWeb3(AsyncHTTPProvider(RPC_URL, request_kwargs={"timeout": 90}))
    if not await w3.is_connected():
        raise SystemExit(f"RPC down: {RPC_URL}")

    store = await open_store()
    if isinstance(store, SqliteStore):
        await store.setup()
    try:
        await index_wallet(w3, store)
        ok = await verify(w3, store)
        raise SystemExit(0 if ok else 2)
    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
