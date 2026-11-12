#!/usr/bin/env python3
"""用钱包私钥生成 Polymarket CLOB API 凭据 (只在你自己的机器上运行, 不要把私钥发给任何人)
    pip install py-clob-client-v2
    export POLY_PRIVATE_KEY=0x...      # 建议用一个新建的空钱包, 仅用于读行情
    python3 derive_creds.py
"""
import os
from py_clob_client_v2 import ClobClient

c = ClobClient(host="https://clob.polymarket.com", chain_id=137, key=os.environ["POLY_PRIVATE_KEY"])
cr = c.create_or_derive_api_key()
g = lambda *n: next((getattr(cr, k, None) or (cr.get(k) if isinstance(cr, dict) else None)) for k in n)
print(f"export POLY_API_KEY={g('api_key','apiKey','key')}")
print(f"export POLY_API_SECRET={g('api_secret','secret')}")
print(f"export POLY_API_PASSPHRASE={g('api_passphrase','passphrase')}")
