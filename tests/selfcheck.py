"""Runnable self-check for the parsing helpers. Run: .venv/bin/python tests/selfcheck.py"""

import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.main import _abi_string, _first, _num, _pad, _word, bsc_address, normalise_tx, tx_rows  # noqa: E402

WIB = ZoneInfo("Asia/Jakarta")

# ABI word decoding
assert _word("0x" + "0" * 63 + "5") == 5
assert _word("0x" + "0" * 62 + "1a" + "0" * 64, 0) == 26
# string return: offset 32, length 5, "NONCE"
STR = "0x" + (32).to_bytes(32, "big").hex() + (5).to_bytes(32, "big").hex() + b"NONCE".hex().ljust(64, "0")
assert _abi_string(STR) == "NONCE"
assert _abi_string("0xdeadbeef") == ""

# address padding
assert _pad("0xAbC0000000000000000000000000000000000001") == "0" * 24 + "abc0000000000000000000000000000000000001"

# first-non-empty lookup
assert _first({"a": None, "b": 7}, "a", "b") == 7
assert _first({}, "x", default="d") == "d"

# history list shapes (non-dict rows are dropped, never passed through)
assert tx_rows({"transactions": [{"a": 1}]}) == [{"a": 1}]
assert tx_rows({"list": [{"b": 2}]}) == [{"b": 2}]
assert tx_rows({"nope": 1}) == []
assert tx_rows("junk") == []

# BSC row must win over the Solana row BAW lists first
assert bsc_address([{"binanceChainId": "CT_501", "address": "SoL"}, {"binanceChainId": 56, "address": "0xBSC"}]) == "0xBSC"
assert bsc_address([]) is None

# tx legs + fee are nested under txHashList[].instructions / .networkFee
tx = normalise_tx({
    "txType": "transfer", "txHash": "0xaa", "txTime": "2026-10-28T01:12:57+07:00",
    "binanceChainId": "56", "status": "confirmed",
    "txHashList": [{
        "networkFee": {"feeTokenSymbol": "BNB", "feeValue": "26952820615624", "feeTokenDecimals": 18},
        "instructions": {"send": [], "receive": [
            {"amount": "1", "tokenInfo": {"contractAddress": "0xFb", "symbol": "HASH", "tokenId": "4179", "decimals": 0}}]},
    }],
})
assert tx["symbol"] == "HASH" and tx["tokenId"] == "4179" and tx["amount"] == 1.0
assert tx["direction"] == "receive" and tx["type"] == "transfer"  # NFT receive stays a transfer
assert abs(tx["fee"]["amount"] - 26952820615624 / 1e18) < 1e-15 and tx["fee"]["symbol"] == "BNB"
assert datetime.fromisoformat(tx["time"]).tzinfo is not None
# BAW never reports per-tx USD value: it must stay null, not 0.0 or a guess
assert tx["valueUsd"] is None

# unsolicited fungible receive (fee paid, nothing sent) is an airdrop, not a transfer
drop = normalise_tx({
    "txType": "transfer", "txHash": "0xcc", "status": "confirmed",
    "txHashList": [{
        "networkFee": {"feeTokenSymbol": "BNB", "feeValue": "1050000000000", "feeTokenDecimals": 18},
        "instructions": {"send": [], "receive": [
            {"amount": "149", "tokenInfo": {"contractAddress": "0xSo", "symbol": "SOLO", "decimals": 18}}]},
    }],
})
assert drop["type"] == "airdrop" and drop["amount"] == 149 and drop["legs"][0]["decimals"] == 18
# a real send stays a transfer
sent = normalise_tx({
    "txType": "transfer", "txHash": "0xdd",
    "txHashList": [{"networkFee": {"feeValue": "1"}, "instructions": {
        "send": [{"amount": "1", "tokenInfo": {"symbol": "BNB", "decimals": 18}}], "receive": []}}],
})
assert sent["type"] == "transfer"

# empty instructions must not crash and must not invent legs
empty = normalise_tx({"txType": "contract_call", "txHash": "0xbb", "status": "failed",
                      "txHashList": [{"instructions": {"send": [], "receive": []}}]})
assert empty["symbol"] is None and empty["legs"] == [] and empty["fee"] is None

# missing fields stay None
blank = normalise_tx({})
assert blank["hash"] is None and blank["time"] is None and blank["valueUsd"] is None

# balance rows are a bare list under `data`, with `address` as the contract key
assert _num("0.00663") == 0.00663
assert _first({"address": "0xEeee"}, "contractAddress", "tokenAddress", "address") == "0xEeee" 

print("selfcheck ok")