# -*- coding: utf-8 -*-
"""
An application to download bitmex's data with fine resolution. Default are in parentheses
Pour charger les fichiers téléchargés utiliser btxDataLoader.py
"""
from time import sleep
from typing import Tuple

import argparse
import logging
import os
import platform  # handle os check
import sys
import requests as rq
from pandas import DataFrame, Timestamp, Timedelta
from pathlib import Path

from getBitMEXData.btx_types import bucketT, oTimestampT, symbolT
from getBitMEXData.settings import (
    STRF,
    LIVE_URL,
    TEST_URL,
    TC,
    STARTDATE_DFT,
    SYMBOL_DFT,
    ENTRYPOINT_DFT,
    LOGLEVEL_DFT,
    ENDTIME_DFT,
    STARTTIME_DFT,
    BINSIZE_DFT,
    PAUSE_DFT,
    COUNT_DFT,
    FOLDER_DFT,
)

# setting the default time zone for the system
if platform.system() == "Linux":
    # Time.tzset ne fonctionne qu'avec UNIX le mettre en commentaire pour windows
    # mais veillé à spécifier la date de départ et de fin des messages.  avec les option --startTime et --endTime
    OS_TZ = os.environ.get("TZ", "UTC")
else:
    # in the case os.environ does not exist
    OZ_TZ = "UTC"

logger = logging.getLogger()
logger.setLevel("INFO")

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
    pause: float = 0.5,
    binSize: bucketT = "1d",
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
    assert Q or (binSize and symbol and reverse), (
        "Either Q is set or binSize, symb and reverse",
        f"but Q:{Q} or (binSize:{binSize} and symbol:{symbol} and reverse:{reverse})",
    )
    logger.debug(
        f"Got {Q}, folder={fout}, startTime={startTime}, endTime={endTime},"
        f" binSize={binSize}, pause={pause}"
    )

    # Init session and defaults settings
    auth = None  # auth = APIKeyAuthWithExpires(apiKey, apiSecret)
    sess = init_session()
    fout = (
        f"./{symbol}-{binSize}-{endTime.strftime(STRF)}.csv" if fout is None else fout
    )
    # prise en compte de windows
    fout = Path(fout)

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

    if startTime is None:
        startTime = Q["startTime"]
    else:
        Q["startTime"] = Timestamp(startTime).round(TC[binSize])

    # Ready to open the file to make several requests and write results
    with open(fout, "w") as fd:
        Q, firstReqDate, lastReqDate = request_write_nlog(
            Q, sess, auth, url, fd, header=True, pause=0
        )
        logging.debug(f"Req 0: Q={Q})")
        logging.info(f"{firstReqDate.strftime(STRF)}:{lastReqDate.strftime(STRF)}")

        i = 1
        while not (reached(lastReqDate, endTime) or firstReqDate == lastReqDate):
            Q, firstReqDate, lastReqDate = request_write_nlog(
                Q, sess, auth, url, fd, step=i, startTime=lastReqDate, pause=pause
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
    # print(logmsg, end="\r")

    return query, firstReqDate, lastReqDate


def reached(lastReqDate, endTime=None):
    """
    Returns True si endTime <= lastReqDate (end more recent than last)
    or if endTime is None
    """
    endTz = OS_TZ if endTime is None or endTime.tz is None else endTime.tz
    lastTz = OS_TZ if lastReqDate.tz is None else lastReqDate.tz

    _endTime = tz_enforced(endTime, endTz)
    _lastReqDate = tz_enforced(lastReqDate, lastTz)

    return endTime is None or _endTime <= _lastReqDate


def tz_enforced(timestamp_, timezone_):
    """Make sure timestamp has the timezone _timezone else raise Exception"""
    try:
        _timestamp = Timestamp(timestamp_).tz_convert(timezone_)
    except TypeError as te:
        if "tz-naive" in te.__repr__():
            _timestamp = Timestamp(timestamp_).tz_localize(timezone_)
        else:
            raise te

    return _timestamp


def parse_args():
    """Settings the applications's arguments and options."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--folder",
        "-f",
        help=(
            "folder to which we save the results.  The file format is "
            "<symbol>-<BINSIZE>-<LAST_RECORD_DATE>.csv"
        ),
        default=FOLDER_DFT,
    )
    parser.add_argument(
        "--count",
        "-c",
        type=int,
        help="Max number of record per requests.",
        default=COUNT_DFT,
    )
    parser.add_argument(
        "--pause",
        "-p",
        type=float,
        help=("Minimun waiting time between 2 requests. Avoid overloading the server"),
        default=PAUSE_DFT,
    )
    parser.add_argument(
        "--binSize",
        "-b",
        help="Resolution bin size, can also be 1m, 5m, 1h.",
        default=BINSIZE_DFT,
    )
    parser.add_argument(
        "--startTime",
        "-s",
        help=(
            "Start time of data collection (oldest available 2016-05-05"
            " 04:00:00 'UTC').  Check time zones"
        ),
        default=STARTTIME_DFT,
    )
    parser.add_argument(
        "--endTime",
        "-e",
        help=(
            "End time of data collection.  ( now - 1 unit of chosen"
            " resolution)-05-05 04:00:00 'UTC').  Check TZ"
        ),
        default=ENDTIME_DFT,
    )
    parser.add_argument(
        "--live",
        "-l",
        action="store_true",
        help="If present use LIVE keys else bitmex testnet.",
    )
    parser.add_argument(
        "--logLevel", "-L", help="set the log level", default=LOGLEVEL_DFT
    )
    parser.add_argument(
        "--entryPoint",
        "-E",
        help=("Set the path to append to the LIVE or TEST url before the query."),
        default=ENTRYPOINT_DFT,
    )
    parser.add_argument(
        "--symbol",
        "-S",
        help=(
            "Symbol for which to get historical data.  "
            "Note: default start time may change depending on this"
        ),
        default=SYMBOL_DFT,
    )

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
    # localising the timezone
    startTime = startTime.tz_localize(OS_TZ)

    # To avoid empty request we stop one unit befor the present date.
    endTime = (
        (Timestamp.now() - Timedelta(nUnit, tUnit)).round(timeUnit)
        if args.endTime is None
        else Timestamp(args.endTime)
    )

    # making_sure the format is also valid for windows
    endTime = endTime.tz_localize(OS_TZ)  # .strftime(STRF)

    query = {
        "binSize": args.binSize,
        "count": args.count,
        "partial": "false",
        "reverse": "false",
        "symbol": args.symbol,
    }

    _fout = Path(args.folder).joinpath(
        f"{args.symbol}-{args.binSize}-{endTime.strftime(STRF)}"
    )
    # kwargs stand for key words arguments
    kwargs = {
        "fout": f"{_fout}",
        "endTime": endTime,
        "startTime": startTime,
        "pause": args.pause,
    }

    # use live or test ids
    URL = URLS[args.live]

    logger.warning(f"Writting data to {kwargs['folder']}")

    _ = get_bucketed_trades(url=f"{URL}{args.entryPoint}", Q=query, **kwargs)
    return None


if __name__ == "__main__":
    main_prg()
    sys.exit()
