# RM Invest — Polymarket wallet restore

Кошелёк: `0x46b353667fd7d846af3bbeda6584b0e5b883d3de`

Только Polygon RPC (`eth_getLogs` + `eth_call`) → PostgreSQL. Без Polymarket API и без индексаторов.

## Запуск

```bash
# 1) Postgres
docker compose up -d

# 2) deps
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# 3) прогон (нужен archive RPC)
set POLYGON_RPC_URL=https://gateway.tenderly.co/public/polygon
set DATABASE_URL=postgresql://polymarket:polymarket@127.0.0.1:5433/polymarket
python -u restore_wallet.py
```

Повторный запуск догоняет с сохранённого `tip_block`.

Без Docker:

```bash
set USE_SQLITE=1
set POLYGON_RPC_URL=https://gateway.tenderly.co/public/polygon
python -u restore_wallet.py
```

Только сверка:

```bash
set VERIFY_ONLY=1
python -u restore_wallet.py
```

Полный переиндекс с нуля:

```bash
set FORCE_FULL=1
python -u restore_wallet.py
```

Частичная история:

```bash
set START_BLOCK=90000000
```

> `polygon-bor.publicnode.com` часто prune'ит историю — для полной сверки нужен archive RPC (Tenderly public / свой node).

## Что индексируется

- ERC-20 Transfer: USDC.e, native USDC, pUSD
- ERC-1155 TransferSingle/Batch: Conditional Tokens (`0x4D97...`)

Балансы считаются из `transfers` и сверяются с `balanceOf` / `balanceOfBatch` → таблица `balance_check`.
