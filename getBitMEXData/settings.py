# -*- coding: utf-8 -*-
"""Settings.py."""
LIVE_URL = "https://www.bitmex.com/api/v1/"
TEST_URL = "https://testnet.bitmex.com/api/v1/"

# Duration converter
TC = {"1m": "60s", "5m": "300s", "1h": "1H", "1d": "1D"}
STRF = "%Y-%m-%dT%H_%M"  # default time format for saving the data


# the oldest date I know off but 2015-09-26 for bitmex
# Should be in UTC
STARTDATE_DFT = {"XBTUSD": "2016-05-05 04:00", "ADAM20": "2020-03-14"}

PRICE_TICKLOG = {"XBT": 1, "ADA": 8}

# default arguments
FOLDER_DFT = "./"
COUNT_DFT = 600
PAUSE_DFT = 1.2
BINSIZE_DFT = "1d"
STARTTIME_DFT = None
ENDTIME_DFT = None
LOGLEVEL_DFT = "WARNING"
ENTRYPOINT_DFT = "trade/bucketed"
SYMBOL_DFT = "XBTUSD"
