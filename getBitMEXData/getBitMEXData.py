# -*- coding: utf-8 -*-
"""main file getBitMEXData.py."""
from time import sleep
from typing import Tuple
import argparse
import logging
import os
import time

import requests as rq
from pandas import DataFrame, Timestamp, Timedelta

from getBitMEXData.getBitMEXtypes import bucketT, oTimestampT, symbolT
from getBitMEXData.settings import LIVE_URL, TEST_URL, TC, STARTDATE_DFT

os.environ["TZ"] = "UTC"
time.tzset()

logger = logging.getLogger()
STRF = "%Y-%m-%dT%H:%M"  # default time format for saving the data

# Converts bitmex time unit to pd.timestamp time units
URLS = {
    True: LIVE_URL,
    False: TEST_URL,
}


def init_session(name="foo"):
    """Set a session with name `foo`."""
    sess = rq.Session()
    sess.headers.update({"user-agent": f"{name}-"})
    sess.headers.update({"content-type": "application/json"})
    sess.headers.update({"accept": "application/json"})
    return sess


def make_request(query, sess, auth, url, verb="GET"):
    """
    Make the request with query been passed via rest in sessions sess.

    - request verb  (default GET)
    """
    rep, req = None, None
    try:
        req = rq.Request(verb, f"{url}", auth=auth, params=query)
        print(req.url, query)
        prepp = sess.prepare_request(req)
        rep = sess.send(prepp, timeout=10)
        rep = rep.json()
        logmsg = (
            f"Req: {req}, url={req.url}, parms={query}, auth={auth}"
            f"Prepp: {prepp}: body={prepp.body}, header={prepp.headers},"
            f" url={prepp.path_url}"
        )
        logger.debug(logmsg)
    except Exception as e:
        if req:
            logmsg = (
                f"#### ERROR ####\nReq: req={req}, url={req.url}, params={query}"
                f"\nPrepp: header={prepp.headers}, url={prepp.path_url}"
            )
            logmsg += f"\nRep: {rep}"

            logger.exception(logmsg)
        raise e

    return rep


def get_time_window(rep, reverse=False) -> Tuple[Timestamp, Timestamp]:
    """
    Return first and last date of a request response.

    - reverse if False (default), oldest date is first,
    """
    try:
        old = Timestamp(rep[0]["timestamp"])
        recent = Timestamp(rep[-1]["timestamp"])
    except (KeyError, IndexError) as e:
        logger.error(f"rep={rep}, probably empty.")
        raise e

    return (recent, old) if reverse else (old, recent)


def get_bucketed_trades(
    url,
    Q=None,
    fout=None,
    startTime: oTimestampT = None,
    endTime: oTimestampT = None,
    binSize: bucketT = "1d",
    pause: float = 0.5,
    reverse: str = "false",
    symbol: symbolT = "XBTUSD",
):
    """
    Returns the historical data from bitMEX (default).

    resulting values are : timestamp, symbol, open, high, low, close, trades, 
    volume,  vwap,  lastSize, turnover, homeNotional, foreignNotional,

    - url : live or test url
    Times are in isoformat eg. 2016-12-27T11:00Z
    - `startTime`: date of first record to download
    - `endTime` : date of the last recorde to download 
    - binSize : bucketed size to ask: one of 1m, 5m, 1h, 1d    
    
    Params:
    - Q : The Query requested.
    should be a dictionnary with keys binSize, partial, symbol, count and reverse.
    - fout : the name of the file to write to the results to
    - pause : to throttle the requests and avoid been rejected by bitMEX
    - reverse : should we return earliest data first ?
    """
    logger.debug(
        f"Got {Q}, fout={fout}, startTime={startTime}, endTime={endTime},"
        f" binSize={binSize}, pause={pause}"
    )

    # Init session and defaults settings
    auth = None  # auth = APIKeyAuthWithExpires(apiKey, apiSecret)
    sess = init_session()
    fout = (
        "./btxData-{binSize}-{endTime.strftime('%Y%m%dT%H:%M')}.csv"
        if fout is None
        else fout
    )
    Q = (
        {
            "binSize": binSize,
            "partial": "false",
            "symbol": symbol,
            "count": 180,
            "reverse": reverse,
        }
        if Q is None
        else Q
    )

    if startTime is not None:
        Q["startTime"] = Timestamp(startTime).round(TC[binSize])
    else:
        startTime = Q["startTime"]

    # Ready to open the file make requests and write results
    with open(fout, "w") as fd:
        Q, firstReqDate, lastReqDate = request_write_nlog(
            Q, sess, auth, url, fd, header=True, pause=0
        )
        logging.warning(
            f"Req 0: Q={Q}, {firstReqDate.strftime(STRF)}"
            f" --> {lastReqDate.strftime(STRF)}"
        )

        i = 1
        while not reached(lastReqDate, endTime):
            Q, firstReqDate, lastReqDate = request_write_nlog(
                Q, sess, auth, url, fd, step=i, startTime=lastReqDate, pause=pause
            )
            print(
                f"Req {i}: {firstReqDate.strftime(STRF)}"
                f" --> {lastReqDate.strftime(STRF)}",
                end="\r",
            )
            i += 1

    # last log before exit
    Q["startTime"], Q["endTime"] = startTime, endTime
    logging.warning(f"Finished in {i} requests for query={Q}")

    return sess


def request_write_nlog(
    query, sess, auth, url, fd, header=False, pause=1, step=0, startTime=None
):
    """
    Make the requests and write the results in a file descriptor.

    returns the query and 2 timestamps.
    """
    logger.debug(f"Requesting {query}")
    # pause to avoid been rejected, below 1.2 s between requests,
    # it can be rejected by server after a few tens of requests.
    sleep(pause)

    if startTime is not None:
        query["startTime"] = startTime
    try:
        rep = make_request(query, sess, auth, url)
        firstReqDate, lastReqDate = get_time_window(rep)
    except (KeyError, IndexError):
        # Then we probably have no data for this request, we jump to next day
        firstReqDate = Timestamp(query.get("startTime"))
        lastReqDate = Timestamp(firstReqDate) + Timedelta("1D")
        logmsg = (
            f"# Empty Response: Step={step}, {firstReqDate.strftime(STRF)}"
            f" --> {lastReqDate.strftime(STRF)}.\n"
        )
    except Exception:
        logger.error(f"query={query}, auth={auth}, url={url}")
        raise

    else:
        df = DataFrame(rep).set_index("timestamp")
        df.to_csv(fd, header=header)
        logmsg = (
            f"# Step={step}, {firstReqDate.strftime(STRF)}"
            f" --> {lastReqDate.strftime(STRF)}.\n"
        )

    fd.write(logmsg)
    print(logmsg, end="\r")

    return query, firstReqDate, lastReqDate


def reached(lastReqDate, endTime=None):
    """
    Returns True si endTime <= lastReqDate (end more recent than last)
    or if endTime is None
    """
    # Check the tz settings
    endTz = os.environ["TZ"] if endTime.tz is None else None
    lastTz = os.environ["TZ"] if lastReqDate.tz is None else None
    return endTime is None or Timestamp(endTime, tz=endTz) <= Timestamp(
        lastReqDate, tz=lastTz
    )


def parse_args():
    """Settings the applications's arguments and options."""

    description = """An application to download bitmex's data with what ever resolution you need."""
    fout_default = "btxData"
    fout_help = f"base Name of the csv file where to save the results. (default {fout_default}-freq-lastrecorddate.csv)"
    count_default = 600
    count_help = "Max number each of records in requests (default 600)"
    pause_default = 1.2
    pause_help = "Min time to wait between 2 requests (default 1.2).  to avoid overloading the server"
    binSize_default = "1d"
    binSize_help = "Bin size or type requested, or time resolution (default 1d), can also be 1m, 5m, 1h."
    startTime_default = None
    startTime_help = "Time to start the data collection (default, oldest available 2016-05-05 04:00:00 'UTC').  Check time zones"
    endTime_default = None
    endTime_help = "Time to end the data collection (default, now - 1 unit of chosen resolution)-05-05 04:00:00 'UTC').  Check TZ"
    logLevel_default = "WARNING"
    logLevel_help = "set the log level"
    live_help = "If present use LIVE keys to get the data else use the test site."
    entryPoint_default = "trade/bucketed"
    entryPoint_help = "Set the entry level.  the path to append to the LIVE or TEST url before the query"
    symbol_help = "Set the symbol for which to get historical data def. XBTUSD.  default start date may change depending on symbol"
    symbol_default = "XBTUSD"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--fout", "-f", help=fout_help, default=fout_default)
    parser.add_argument(
        "--count", "-c", type=int, help=count_help, default=count_default
    )
    parser.add_argument(
        "--pause", "-p", type=float, help=pause_help, default=pause_default
    )
    parser.add_argument("--binSize", "-b", help=binSize_help, default=binSize_default)
    parser.add_argument(
        "--startTime", "-s", help=startTime_help, default=startTime_default
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


def main_prg():
    """Run the main programme."""
    args = parse_args()

    logger.setLevel(args.logLevel)

    timeUnit = TC[args.binSize]
    nUnit, tUnit = int(timeUnit[:-1]), timeUnit[-1]

    startTime = (
        Timestamp(STARTDATE_DFT[args.symbol]).round(timeUnit)
        if args.startTime is None
        else Timestamp(args.startTime)
    )
    # To avoir empty request we stop one unit befor the present date.
    endTime = (
        (Timestamp.now() - Timedelta(nUnit, tUnit)).round(timeUnit)
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
    URL = URLS[args.live]

    _ = get_bucketed_trades(url=f"{URL}{args.entryPoint}", Q=query, **kwargs)
    return None


if __name__ == "__main__":
    main_prg()
