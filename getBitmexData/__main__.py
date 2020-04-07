# -*- coding: utf-8 -*-
"""Module getting bitMEX's historical data."""
import argparse
import logging

from configparser import ConfigParser
from importlib import resources

from pandas import Timestamp, Timedelta
from settings import (
    LIVE_KEY,
    LIVE_SECRET,
    LIVE_URL,
    TEST_KEY,
    TEST_SECRET,
    TEST_URL,
)
from getBitmexData import get_bucketed_trades

# Converts bitmex time unit to pd.timestamp time units

logger = logging.getLogger()
STRF = "%Y-%m-%dT%H:%M"  # default time format for saving the data

# Duration converter
TC = {"1m": "60s", "5m": "300s", "1h": "1H", "1d": "1D"}


def read_settings():

    _cfg = ConfigParser()
    _cfg.read_string(resources.read_text("getBitmexData", "settings.cfg"))

    rep = {}

    for label in ["URL", "KEY", "SECRET"]:
        rep[True] = _cfg.get("getBitmexData", f"LIVE_{label}")
        rep[False] = _cfg.get("getBitmexData", f"TEST_{label}")

    return rep


def parse_args():
    """Parse the applications's arguments and options."""
    description = """An application to download bitmex's data with what ever resolution you need."""
    fout_default = "btxData"
    fout_help = (
        f"base Name of the csv file where to save the results."
        " (default {fout_default}-freq-lastrecorddate.csv)"
    )
    count_default = 600
    count_help = "Max number each of records in requests (default 600)"
    pause_default = 1.2
    pause_help = (
        "Min time to wait between 2 requests (default 1.2)."
        "  to avoid overloading the server"
    )
    binSize_default = "1d"
    binSize_help = (
        "Bin size or type requested, or time resolution (default 1d),"
        " can also be 1m, 5m, 1h."
    )
    startTime_default = None
    startTime_help = (
        "Time to start the data collection. (default, oldest"
        " available 2016-05-05 04:00:00 'UTC').  Check time zones"
    )
    endTime_default = None
    endTime_help = (
        "Time to end the data collection (default, now - 1 unit of"
        " chosen resolution)-05-05 04:00:00 'UTC').  Check TZ"
    )
    logLevel_default = "WARNING"
    logLevel_help = "set the log level"
    live_help = "If present use LIVE keys to get the data else use the test site."
    entryPoint_default = "trade/bucketed"
    entryPoint_help = (
        "Set the entry level.  the path to append to the LIVE or"
        " TEST url before the query"
    )
    symbol_help = (
        "Set the symbol for which to get historical data def. XBTUSD. "
        " Default start date may change depending on symbol"
    )
    symbol_default = "XBTUSD"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--fout", "-f", help=fout_help, default=fout_default)
    parser.add_argument(
        "--count", "-c", type=int, help=count_help, default=count_default
    )
    parser.add_argument(
        "--pause", "-p", type=float, help=pause_help, default=pause_default
    )
    parser.add_argument("--binSize", "-s", help=binSize_help, default=binSize_default)
    parser.add_argument(
        "--startTime", "-t", help=startTime_help, default=startTime_default
    )
    parser.add_argument("--endTime", "-e", help=endTime_help, default=endTime_default)
    parser.add_argument("--live", "-l", action="store_true", help=live_help)
    parser.add_argument(
        "--logLevel", "-L", help=logLevel_help, default=logLevel_default
    )
    parser.add_argument(
        "--entryPoint", "-E", help=entryPoint_help, default=entryPoint_default
    )

    parser.add_argument("--symbol", "-S", help=symbol_help, default=symbol_default)

    return parser.parse_args()


def main():
    args = parse_args()

    logger.setLevel(args.logLevel)

    timeUnit = TC[args.binSize]
    nUnit, tUnit = int(timeUnit[:-1]), timeUnit[-1]

    # the oldest date I know off but 2015-09-26 for bitmex
    # I have an issue with tz somewhere

    defStartDate = {"XBTUSD": "2016-05-05 04:00", "ADAM20": "2019-01-01 04:00"}

    startTime = (
        Timestamp(defStartDate[args.symbol]).round(timeUnit)
        if args.startTime is None
        else Timestamp(args.startTime)
    )
    endTime = (
        (Timestamp.now() - Timedelta(1, tUnit)).round(timeUnit)
        if args.endTime is None
        else Timestamp(args.endTime)
    )
    query = {
        "binSize": args.binSize,
        "count": args.count,
        "partial": "false",
        "reverse": "false",
        "symbol": args.symbol,
    }
    kwargs = {
        "endTime": endTime,
        "fout": f"{args.fout}",
        "pause": args.pause,
        "startTime": startTime,
    }

    # use live or test ids
    URL, KEY, SECRET = read_settings()[args.live]

    sess = get_bucketed_trades(
        KEY, SECRET, f"{URL}{args.entryPoint}", Q=query, **kwargs
    )


if __name__ == "__main__":
    main()
