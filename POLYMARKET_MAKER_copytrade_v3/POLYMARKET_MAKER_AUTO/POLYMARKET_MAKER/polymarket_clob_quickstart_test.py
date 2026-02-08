#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""polymarket_clob_quickstart_test.py

极简复现脚本（基于官方 py-clob-client Quickstart）：
- 使用环境变量中的私钥 / 代理地址 / 签名类型初始化 ClobClient
- 调用 create_or_derive_api_creds() 派生 API Key
- 调用一个需要 L2 鉴权的私有接口 get_orders()，验证是否出现 401 / Invalid api key

用法（示例）：
  export POLY_KEY="你的 EOA 私钥（可带 0x）"
  export POLY_FUNDER="你的 Polymarket 资金地址（Proxy 地址）"
  export POLY_SIGNATURE=2   # MetaMask+代理，一般为 2
  python3 polymarket_clob_quickstart_test.py
"""

import os
import sys

from py_clob_client.client import ClobClient
from py_clob_client.constants import POLYGON
from py_clob_client.clob_types import OpenOrderParams
from py_clob_client.exceptions import PolyApiException


def main() -> None:
    host = os.getenv("POLY_HOST", "https://clob.polymarket.com")
    key = os.getenv("POLY_KEY")
    funder = os.getenv("POLY_FUNDER")
    sig_type_str = os.getenv("POLY_SIGNATURE", "2")

    if not key or not funder:
        print("[ERR] POLY_KEY 或 POLY_FUNDER 未设置，请先在环境变量中配置。", file=sys.stderr)
        sys.exit(1)

    try:
        signature_type = int(sig_type_str)
    except ValueError:
        print(f"[ERR] 无法解析 POLY_SIGNATURE='{sig_type_str}'，请设置为 0/1/2。", file=sys.stderr)
        sys.exit(1)

    # 兼容带 0x 的私钥
    key_clean = key[2:] if key.startswith(("0x", "0X")) else key

    print("[STEP] 初始化 ClobClient …")
    print(f"       host={host}")
    print(f"       chain_id={POLYGON}")
    print(f"       signature_type={signature_type}")
    print(f"       funder={funder}")

    client = ClobClient(
        host,
        key=key_clean,
        chain_id=POLYGON,
        signature_type=signature_type,
        funder=funder,
    )

    # 检查公开接口是否正常
    try:
        ok = client.get_ok()
        server_time = client.get_server_time()
        print(f"[CHECK] /ok -> {ok}")
        print(f"[CHECK] server time -> {server_time}")
    except PolyApiException as e:
        print(f"[ERR] 访问公开接口失败：{e}", file=sys.stderr)
        sys.exit(1)

    # L1：派生或创建 API 凭证
    print("[STEP] 派生或创建 API 凭证 …")
    try:
        api_creds = client.create_or_derive_api_creds()
    except PolyApiException as e:
        print(f"[ERR] create_or_derive_api_creds 失败：{e}", file=sys.stderr)
        sys.exit(1)

    client.set_api_creds(api_creds)

    print("[INFO] API 凭证派生成功（为避免泄露，仅打印部分信息）：")
    key_preview = getattr(api_creds, "key", None) or "<missing>"
    secret = getattr(api_creds, "secret", "") or ""
    passphrase = getattr(api_creds, "passphrase", "") or ""
    secret_preview = (secret[:6] + "..." + secret[-4:]) if secret else "<missing>"
    pass_preview = (passphrase[:6] + "..." + passphrase[-4:]) if passphrase else "<missing>"
    print(f"       api_key={key_preview}")
    print(f"       secret≈{secret_preview}")
    print(f"       passphrase≈{pass_preview}")

    # L2：调用需要 API Key 的私有接口
    print("[STEP] 调用需要 L2 鉴权的私有接口 get_orders() …")
    try:
        # 这里使用官方文档里的示例 market ID。即使你在该市场没有挂单，
        # 请求也应该成功，只是返回空列表；若这里 401 / Invalid api key，
        # 说明问题出在 API Key 鉴权层。
        params = OpenOrderParams(
            market="0xbd31dc8a20211944f6b70f31557f1001557b59905b7738480ca09bd4532f84af"
        )
        resp = client.get_orders(params)
        print("[SUCCESS] get_orders 调用成功。返回示例：")
        print(resp)
    except PolyApiException as e:
        print(f"[ERR] get_orders 调用失败：{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
