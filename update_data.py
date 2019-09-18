# -*- coding: utf-8 -*-
""" Fichier avec des fonctions pour récupérer les données historique de BitMex en ligne """
import argparse
import subprocess as sp
import pandas as pd
from kola.settings import (
    LIVE_KEY,
    LIVE_SECRET,
    LIVE_URL,
    TEST_KEY,
    TEST_SECRET,
    TEST_URL,
)
import logging
from kola.utils.datefunc import TC  # time conversion
import os
import time
try:
    import getBitmexData.utils as u
    from getBitmexData.getBitmexData import get_bucketed_trades
except ModuleNotFoundError:
    import utils as u
    from getBitmexData import get_bucketed_trades
    

os.environ["TZ"] = "UTC"
time.tzset()

# {"1m": "60s", "5m": "300s", "1h": "1H", "1d": "1D"}

logger = logging.getLogger()
STRF = "%Y-%m-%d %H:%M"  # default time format

# Converts bitmex time unit to pd.timestamp time units
URLS = {
    True: (LIVE_URL, LIVE_KEY, LIVE_SECRET),
    False: (TEST_URL, TEST_KEY, TEST_SECRET),
}


def get_last_record(fullname):
    """ Renvois le dernier enregistrement d'un fichier btxData with fullname"""

    # on récupère les dernières lignes du fichiers
    _lrs = sp.run(f"tail {fullname}".split(), stdout=sp.PIPE).stdout.split(b"\n")

    # on les filtres
    lastRecords = [lr.decode() for lr in _lrs if not lr.startswith(b"#")]

    assert len(
        lastRecords
    ), f"Vérifier qu'il y a bien des enregistrements valides dans fullname={fullname}"

    return lastRecords[-1]


def get_record_date(record):
    """
    Keyword Arguments:
    record -- un enregistrement valid de données historique de bitmex binned en 1m

    Returns: a pd.Timestamp avec la date la plus de l'enregistrement record
    """
    return pd.Timestamp(record.split(",")[0])


def get_recent_record_file_date(fullname):
    """
    Keyword Arguments:
    fullname -- nom du fichier

    Returns: Renvois la date du plus récent enregistrement de fullname
    """
    return get_record_date(get_last_record(fullname))


def get_recent_data(fromdate, binsize="1m", fout="tmp.csv", live=False):
    """
    Keyword Arguments:
    fromdate -- date à laquelle commencer le téléchargement
    fout     --  fichier de sortie
    live  -- use live or test keys

    Returns: Télécharge les données Bitmex par minutes et les stock dans fout
    """
    # {"1m": "60s", "5m": "300s", "1h": "1H", "1d": "1D"}
    binSize = binsize
    timeUnit = TC[binSize]
    _, tUnit = int(timeUnit[:-1]), timeUnit[-1]

    startTime = fromdate
    endTime = (pd.Timestamp.now() - pd.Timedelta(1, tUnit)).round(timeUnit)

    query = {
        "binSize": binSize,
        "count": 600,
        "partial": "false",
        "reverse": "false",
        "symbol": "XBTUSD",
    }
    kwargs = {"endTime": endTime, "fout": fout, "pause": 1.2, "startTime": startTime}

    # use live or test ids
    URL, KEY, SECRET = URLS[live]

    logger.warning(f"{KEY}, {SECRET}, {URL}, {query}, {kwargs}")
    sess = get_bucketed_trades(KEY, SECRET, URL, Q=query, **kwargs)
    return sess


def update_file(fname, fout="./tmp.csv", live=False):
    """
    Keyword Arguments:
    fname -- name of the file to update. should be name btxData-{freq}-{lastudpatedate}.csv

    get the freq from the file name.
    Download new data in tmp.csv
    append tmp.csv to original file but update the name with last updatedate
    Returns: write the fname updated with new data
    """
    # get
    oldDate = get_recent_record_file_date(fname)
    binSize = get_fname_binsize(fname)
    # download data in fout
    get_recent_data(oldDate, binSize, fout, live)
    concat_files(fname, fout, removetmp=True)
    return update_name(fname)


def update_name(fname):
    """
    Keyword Arguments:
    fname -- name of the file to update

    Returns: updates the name of the file with date of the last record in it
    "fout": f"{args.fout}-{args.binSize}-{endTime}.csv"""
    newDate = get_recent_record_file_date(fname).strftime("%Y%m%dT%H:%M")
    simpDate = get_recent_record_file_date(fname).strftime("%Y")
    binSize = get_fname_binsize(fname)
    baseName = get_fname_basename(fname)
    newName = f"{baseName}-{binSize}-{newDate}.csv"
    simpName = f"{baseName}-{binSize}-{simpDate}.csv"
    logger.warning(f'Renaming {fname} -> {newName}\n'
                   f'Updating link {simpName}')
    os.rename(fname, newName)
    try:
        os.remove(simpName)
    except FileNotFoundError:
        pass
    
    os.symlink(newName, simpName)

    return


def get_fname_binsize(fname):
    """ If the name is {base}-{binsize}-{date}.csv
    return the binsize """
    return fname.split("-")[1]


def get_fname_basename(fname):
    """ If the name is {base}-{binsize}-{date}.csv
    return the basename """
    return fname.split("-")[0]


def concat_files(file1, file2, removetmp=False):
    """
    Keyword Arguments:
    file1 -- first file (to be on the top)
    file2 -- second or tail fail
    Files should have the same format
    concat file 2 at the end of file 1
    removeoriginal (False) -- should we remove file1 and 2 after concatenation

    Returns: write a file fname which is the concatenation of file1 and file2
    """
    my_cmd = ["tail", "-n", "+2", file2]
    with open(file1, "a") as f:
        _ = sp.run(my_cmd, stdout=f)
    if removetmp:
        logger.warning(f"Removing {file2}.")
        return sp.run(f"rm {file2}".split(), stdout=sp.PIPE)
    else:
        return _


def main(fname, live):
    """Update the btxData file fname with latest data"""
    update_file(fname, live=live)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Utility to update btxDatafile with latest data"
    )

    fname_help = f"name of the file to update.  if None takes the latest btxDatafile in current directory"
    live_help = f"If present use live historic data"
    logLevel_def = "INFO"
    logLevel_help = f"Set logLevel (default {logLevel_def})"
    parser.add_argument("--fname", "-f", help=fname_help, default=None)
    parser.add_argument("--live", "-l", action="store_true", help=live_help)
    parser.add_argument("--logLevel", "-L", help=logLevel_help, default=logLevel_def)

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logger = logging.getLogger(args.logLevel)

    if args.fname is None:
        def fcond(f):
            return u.fcond(f, pat=r"btxData.*csv$")

        fname = u.get_cond_recent_file(fcond=fcond)
    else:
        fname = args.fname

    logger.warning(f'Running {args.live} with {fname}')
    main(fname, args.live)
