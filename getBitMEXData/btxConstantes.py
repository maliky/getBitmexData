# -*- coding: utf-8 -*-
"""Constantes for bitmex"""

# some default cols
EXECOLS = [
    "orderID",
    "clOrdID",
    "side",
    "orderQty",
    "price",
    "stopPx",
    "execType",
    "ordType",
    "execInst",
    "ordStatus",
    "triggered",
    "transactTime",
]

EXECOLS_L = EXECOLS + ["lastQty", "lastPx", "lastMkt", "commission"]

# to get price in bargain.price
SETTLEMENTPRICES = {"XBTUSD": ".BXBT", "ADAU20": ".BADAXBT30M"}

# used to round price before passing orders
ROOT2SYMB = {"XBT": "XBTUSD", "ADA": "ADAU20"}
PRICE_TICKLOG = {"XBT": 1, "ADA": 8}

PRICE_PRECISION = {"XBTUSD": 0.5, "ADAU20": 1e-8}


# used in condition
PRICELIST_DFT = [
    "fairPrice",
    "lastPrice",
    "markPrice",
    "askPrice",
    "bidPrice",
    "lastMidPrice",
]

INSTRUMENT_PRICES = [
    f"{suf}Price"
    for suf in [
        "max",
        "prevClose",
        "prev",
        "high",
        "low",
        "last",
        "bid",
        "mid",
        "ask",
        "impactBid",
        "impactMid",
        "impactAsk",
        "fair",
        "mark",
        "indicativeSettle",
    ]
] + ["lastPriceProtected"]


# price variation in % to reach to short to the maximun the tail (stop)
# used to set a e^-f(t) fonction controling the tail's size.
MAX_PRICE_VARIATION = {"XBTUSD": 2.6, "ADAU20": 2}
