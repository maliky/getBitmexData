# -*- coding: utf-8 -*-
"""
A function to load in a dataFrame several files that where downloaded with getBitMEXData.main.
The idea is that the folder contains yearly datafile.
"""

import pandas as pd
from getBitMEXData.settings import FOLDER_DFT, SYMBOL_DFT, BINSIZE_DFT
from getBitMEXData.btxConstantes import PRICE_TICKLOG
from mlkHelper.stattimes import timedelta_to_seconds

def load_btxData(years=[2018], bins=BINSIZE_DFT, folder=FOLDER_DFT, symbol=SYMBOL_DFT):
    """
    years_ a list of years (defautl [2018])
    bins : a bin type '1m', '5m', '1d' def ('5m')
    folder: the directory name where btxfile are
    symbol: is base file name, also used for ticker.  should be a symbol
    filename shoud be in the forme '<symbol>-<BINSIZE>-<YEAR>.csv'
    ajoute une collone avg
    renvois le tableau
    """
    # defautl headers
    header_df = (
        "timestamp symbol open high low close trades volume vwap lastSize"
        " turnover homeNotional foreignNotional".split(" ")
    )
    folder = Path(folder)

    # TODO: header à récupérer dans le fichier sinon problème quand the API return columns order change
    df = None

    for year in years:
        fname = folder.joinpath(f"{symbol}-{bins}-{year}.csv")
        _tdf = (
            pd.read_csv(fname, comment="#", names=header_df, low_memory=False)
            .dropna()
            .drop(index=0)
        )

        # On converti manuellement les timestamp car la colonne index
        # n'est pas 'pure' au chargement
        _tdf = _tdf.loc[~_tdf.timestamp.isin(["timestamp"])]
        try:
            # ignoring header line
            _tdf.timestamp.iloc[:] = _tdf.timestamp.apply(pd.Timestamp)
        except ValueError:
            logging.error("Probably trying to convert header")
            pass

        _tdf = _tdf.set_index("timestamp")

        df = _tdf if df is None else pd.concat([df, _tdf], sort=True)

    _symbol = df.symbol.iloc[0]
    df = df.drop(columns="symbol")  # the symbol column

    # harmoniser l'index
    df = df.sort_index()
    df = df.drop_duplicates()

    # creating a full index for the requested bins
    fullIndex = pd.date_range(
        start=df.index[0], end=df.index[-1], freq=timedelta_to_seconds(bins)
    )

    # and the associated dataferame  Full DF

    fdf = pd.DataFrame(index=fullIndex)
    # we make header similare to those downloaded
    columns = set(header_df) - set(["symbol", "timestamp"])
    for c in columns:
        fdf.loc[:, c] = None

    fdf.index.name = "timestamp"
    # import ipdb; ipdb.set_trace()

    # copying in it the data
    fdf.loc[df.index, :] = df

    # and interpolating to remove nan
    fdf = (
        fdf.apply(lambda s: pd.to_numeric(s))
        .interpolate(method="time")
        .round(PRICE_TICKLOG[symbol])
    )

    fdf.loc[:, "avg"] = (fdf.low + fdf.high) / 2
    fdf.loc[:, "amplitude"] = fdf.high - fdf.low

    return fdf
