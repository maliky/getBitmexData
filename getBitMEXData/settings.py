# -*- coding: utf-8 -*-
"""Settings.py."""
LIVE_URL = "https://www.bitmex.com/api/v1/"
TEST_URL = "https://testnet.bitmex.com/api/v1/"

# Duration converter
TC = {"1m": "60s", "5m": "300s", "1h": "1H", "1d": "1D"}

# the oldest date I know off but 2015-09-26 for bitmex
# Should be in UTC
STARTDATE_DFT = {"XBTUSD": "2016-05-05 04:00", "ADAM20": "2020-03-14"}
