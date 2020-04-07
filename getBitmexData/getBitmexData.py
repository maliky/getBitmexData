# -*- coding: utf-8 -*-
"""Download Bitmex historical data with different time resolution."""
from time import sleep
import os
import time
import logging

import requests as rq
import pandas as pd
from .auth import APIKeyAuthWithExpires

# Duration converter
TC = {"1m": "60s", "5m": "300s", "1h": "1H", "1d": "1D"}

os.environ["TZ"] = "UTC"
time.tzset()

logger = logging.getLogger()
STRF = "%Y-%m-%dT%H:%M"  # default time format for saving the data


def init_session(name="foo"):
    """Set a session with name `foo`."""
    sess = rq.Session()
    sess.headers.update({"user-agent": f"{name}-"})
    sess.headers.update({"content-type": "application/json"})
    sess.headers.update({"accept": "application/json"})
    return sess


def make_request(query, sess, auth, url, verb="GET"):
    """
    Request the query via rest with sessions sess.

    -verb:  (default GET)
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
            f"Prepp: {prepp}: body={prepp.body}, header={prepp.headers}, "
            f"url={prepp.path_url}"
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


def get_time_window(rep, reverse=False):
    """
    Return first and last date of a request response.

    - reverse: False (default), oldest date is first,
    Return a tuple of  pd.Timestamp
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
    url,
    Q=None,
    fout=None,
    startTime=None,
    endTime=None,
    binSize="1d",
    pause=0.5,
    reverse="false",
    symbol="XBTUSD",
):
    """
    Return the historical data for XBTUSD (default).

    from `startTime` to `endTime` bucketed by `binSize`
    return columns are: timestamp, symbol,  open,  high,  low,  close,
    trades,  volume,  vwap,  lastSize,
    turnover,  homeNotional,  foreignNotional,
        - binSize (str) is one of 1m, 5m, 1h, 1d
        - Time are in isoformat eg. 2016-12-27T11:00Z

    Params:
    apiKey, apiSecret, url, obvious
    Q=None,  The Query requested passed as a dictionnary with keys binSize,
    partial, symbol, count and reverse.
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
        Q["startTime"] = pd.Timestamp(startTime).round(TC[binSize])
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
    Make the requests and write the results in a fd file.

    returns the query and 2 timestamps
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
        firstReqDate = pd.Timestamp(query.get("startTime"))
        lastReqDate = pd.Timestamp(firstReqDate) + pd.Timedelta("1D")
        logmsg = (
            f"# Empty Response: Step={step}, {firstReqDate.strftime(STRF)}"
            f" --> {lastReqDate.strftime(STRF)}.\n"
        )
    except Exception:
        logger.error(f"query={query}, auth={auth}, url={url}")
        raise

    else:
        df = pd.DataFrame(rep).set_index("timestamp")
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
    Return True si endTime <= lastReqDate.

    (end more recent than last) or if endTime is None
    """
    # Check the tz settings
    endTz = os.environ["TZ"] if endTime.tz is None else None
    lastTz = os.environ["TZ"] if lastReqDate.tz is None else None
    return endTime is None or pd.Timestamp(endTime, tz=endTz) <= pd.Timestamp(
        lastReqDate, tz=lastTz
    )
