# -*- coding: utf-8 -*-
import argparse
import pandas as pd
from time import sleep
import requests as rq
import logging
from auth import APIKeyAuthWithExpires

# create a settings file with your IDs
from settings import (
    LIVE_KEY,
    LIVE_SECRET,
    LIVE_URL,
    TEST_KEY,
    TEST_SECRET,
    TEST_URL,
)


# Duration converter
TC = {"1m": "60s", "5m": "300s", "1h": "1H", "1d": "1D"}

logger = logging.getLogger()
STRF = "%Y-%m-%d %H:%M"  # default time format for saving the data

# switch between live and test
URLS = {
    True: (LIVE_URL, LIVE_KEY, LIVE_SECRET),
    False: (TEST_URL, TEST_KEY, TEST_SECRET),
}


def init_session(name="foo"):
    """Set a session with name `foo`"""

    sess = rq.Session()
    sess.headers.update({"user-agent": f"{name}-"})
    sess.headers.update({"content-type": "application/json"})
    sess.headers.update({"accept": "application/json"})
    return sess


def make_request(query, sess, auth):
    """Make a `query` request  passed via REST in session `sess`"""
    rep = None
    try:
        req = rq.Request("GET", f"{URL}trade/bucketed", auth=auth, params=query)
        prepp = sess.prepare_request(req)
        rep = sess.send(prepp, timeout=10)
        rep = rep.json()
        logmsg = (
            f"Req: {req}, url={req.url}, parms={query}, auth={auth}"
            f"Prepp: {prepp}: body={prepp.body}, header={prepp.headers}, url={prepp.path_url}"
        )
        logger.debug(logmsg)
    except Exception as e:
        logger.exception(
            f"#### ERROR ####\nReq: req={req}, url={req.url}, params={query}"
            f"\nPrepp: header={prepp.headers}, url={prepp.path_url}"
            f"\nRep: {rep}"
        )
        raise e

    return rep


def get_time_window(rep, reverse=False):
    """Return the first and last date from the response `rep` of a request response.  
    if False (default), oldest date is first,
    Returns a tuple of pd.Timestamp
    """
    try:
        old = pd.Timestamp(rep[0]["timestamp"])
        recent = pd.Timestamp(rep[-1]["timestamp"])
    except (KeyError, IndexError) as e:
        logger.error(f"rep={rep}, probably empty.")
        raise e

    return (recent, old) if reverse else (old, recent)


def get_bucketed_trades(
    apiKey,
    apiSecret,
    Q=None,
    fout=None,
    startTime=None,
    endTime=None,
    binSize="1d",
    pause=0.5,
    reverse="false",
):
    """
    Returns the historical data for XBTUSD (default) from `startTime` to `endTime` bucketed by `binSize` 
    return columns are: timestamp, symbol,  open,  high,  low,  close,  trades,  volume,  vwap,  lastSize,  
    turnover,  homeNotional,  foreignNotional,
        - binSize (str) is one of 1m, 5m, 1h, 1d
        - Time are in isoformat eg. 2016-12-27T11:00Z

    Params:
    apiKey, apiSecret, obvious
    Q=None,  The Query requested passed as a dictionnary with keys binSize, partial, symbol, count and reverse.
    fout=None, the name of the file to write to
    pause=0.5, to throttle the request
    reverse="false",
"""

    logger.debug(
        f"Got {Q}, fout={fout}, startTime={startTime}, endTime={endTime},"
        f" binSize={binSize}, pause={pause}"
    )

    # Init session and defaults settings
    auth = APIKeyAuthWithExpires(apiKey, apiSecret)
    sess = init_session()
    fout = "./btxData.csv" if fout is None else fout
    Q = (
        {
            "binSize": binSize,
            "partial": "false",
            "symbol": "XBTUSD",
            "count": 180,
            "reverse": reverse,
        }
        if Q is None
        else Q
    )

    if startTime is not None:
        Q["startTime"] = pd.Timestamp(startTime).round(TC[binSize])
    else:
        startTime = Q["startTime"]

    # Ready to open the file, make the request and write the results
    with open(fout, "w") as fd:
        Q, firstReqDate, lastReqDate = request_write_nlog(
            Q, sess, auth, fd, header=True, pause=0
        )
        logging.warning(
            f"Req 0: Q={Q}, {firstReqDate.strftime(STRF)}"
            f" --> {lastReqDate.strftime(STRF)}"
        )

        i = 1
        while not reached(lastReqDate, endTime):
            Q, firstReqDate, lastReqDate = request_write_nlog(
                Q, sess, auth, fd, step=i, startTime=lastReqDate, pause=pause
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
    query, sess, auth, fd, header=False, pause=1, step=0, startTime=None
):
    """Makes the requests and write the results in a fd"""

    logger.debug(f"Requesting {query}")
    # pause to avoid been rejected, below 1.2 s between requests, it can be rejected by server
    # after a few tens of requests.
    sleep(pause)

    if startTime is not None:
        query["startTime"] = startTime
    try:
        rep = make_request(query, sess, auth)
        firstReqDate, lastReqDate = get_time_window(rep)
    except (KeyError, IndexError):
        # Then the server has probably no data for this request, we jump to next day
        firstReqDate = pd.Timestamp(query.get("startTime"))
        lastReqDate = pd.Timestamp(firstReqDate) + pd.Timedelta("1D")
        logmsg = (
            f"# Empty Response: Step={step}, {firstReqDate.strftime(STRF)}"
            f" --> {lastReqDate.strftime(STRF)}."
        )
    else:
        df = pd.DataFrame(rep).set_index("timestamp")
        df.to_csv(fd, header=header)
        logmsg = (
            f"# Step={step}, {firstReqDate.strftime(STRF)}"
            f" --> {lastReqDate.strftime(STRF)}."
        )

    fd.write(logmsg)
    print(logmsg, end="\r")

    return query, firstReqDate, lastReqDate


def reached(lastReqDate, endTime=None):
    """Returns True si endTime <= lastReqDate (end more recent than last)
    or if endTime is None"""
    # Check the tz settings
    return endTime is None or pd.Timestamp(endTime, tz="UTC") <= pd.Timestamp(
        lastReqDate, tz="UTC"
    )


def parse_args():
    """Settings the applications's arguments and options"""

    description = """An application to download bitmex's data with what ever resolution you need."""
    fout_default = "./btxData.csv"
    fout_help = "Name of the csv file where to save the results. (default btxData.csv)"
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

    return parser.parse_args()


if __name__ == "__main__":

    args = parse_args()

    logger.setLevel(args.logLevel)

    timeUnit = TC[args.binSize]
    nUnit, tUnit = int(timeUnit[:-1]), timeUnit[-1]

    query = {
        "binSize": args.binSize,
        "partial": "false",
        "symbol": "XBTUSD",
        "count": args.count,
        "reverse": "false",
    }

    if args.startTime is None:
        # the oldest date I know off but 2015-09-26 for bitmex
        # I have an issue with tz somewhere
        startTime = pd.Timestamp("2016-05-05 04:00").round(timeUnit)
    else:
        startTime = pd.Timestamp(args.startTime)

    if args.endTime is None:
        endTime = (pd.Timestamp.now() - pd.Timedelta(1, tUnit)).round(timeUnit)
    else:
        endTime = pd.Timestamp(args.endTime)

    kwargs = {
        "fout": args.fout,
        "pause": args.pause,
        "binSize": args.binSize,
        "startTime": startTime,
        "endTime": endTime,
    }

    # use live or test ids
    URL, KEY, SECRET = URLS[args.live]

    sess = get_bucketed_trades(KEY, SECRET, Q=query, **kwargs)