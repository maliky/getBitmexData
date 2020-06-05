# import pandas as pd
import os
import os.path as op
import re
import datetime as dt
import logging

from pandas import DataFrame, to_datetime
from pandas.errors import OutOfBoundsDatetime

""" Bibliothèques d'utilitaire pour manipuler les fichiers """


def is_file(fname):
    """Renvois true or false depending if file pas condition."""
    #  isabs isdir isfile islink ismount
    return op.isfile(fname)


def fcond(f, pat):
    """Set a condition on a file name."""
    rpat = re.compile(pat)
    return rpat.search(f)


def is_xls_file(f):
    return fcond(f, pat=r"\.xls$")


def is_csv_file(f):
    return fcond(f, pat=r"\.csv$")


def sort_param(fname):
    """renvois un nombre qui permet de classer le fichier fname"""
    #  getatime getctime *getmtime* getsize
    return op.getmtime(fname)


def get_latest_export_basename(srcdir=None):
    """returns the latest export file name.  If the srcdir is not given returns the cwd"""
    "FOO_Baz_export_dossiers_heure_minute_year_month_day.xls"
    fname = get_cond_recent_file(rang=0, srcdir=srcdir, coln="fname", fcond=is_file)
    bname = "".join(fname.split(".")[:-1])
    return bname


def get_recent_file(rang=0, srcdir=None):
    """soit un rang 0 à nb fichiers dans srcdir, renvois la date du fichier à ce rang class du plus récent au plus ancien.
   srcdir (=cwd by default)"""
    return get_cond_recent_file(rang=rang, srcdir=srcdir, coln="fname", fcond=is_file)


def get_recent_xls_file(rang=0, srcdir=None, dcond=None):
    return get_cond_recent_file(
        rang=rang, srcdir=srcdir, coln="fname", fcond=is_xls_file, dcond=dcond
    )


def get_most_recent_csv_file(srcdir=None):
    return get_cond_recent_file(srcdir=srcdir, rang=0, coln="fname", fcond=is_csv_file)


def get_recent_date(rang=0, srcdir=None):
    """soit un rang 0 à nb fichiers dans srcdir, renvois la date du fichier à ce rang class du plus récent au plus ancien.
   srcdir (=cwd by default)"""
    return get_cond_recent_file(
        srcdir=srcdir, rang=rang, coln="getmtime", fcond=is_xls_file
    )


def get_cond_recent_file(
    rang=0, srcdir=None, coln="fname", fcond=lambda x: op.isfile(x), dcond=None
):
    """soit un rang 0 à nb fichiers dans srcdir, renvois la date du fichier à ce rang class du plus récent au plus ancien si pat (def \.xls$) en regex est dans sont fullname (avec path)
    srcdir (=cwd by default)"""
    return get_cond_recent_info(
        rang=rang, srcdir=srcdir, coln=coln, fcond=fcond, dcond=dcond
    )


def get_cond_recent_info(
    rang=0, srcdir=None, coln="fname", fcond=lambda x: op.isfile(x), dcond=None
):
    """soit un rang 0 à nb fichiers dans srcdir, renvois la date du fichier à ce rang class du plus récent au plus ancien si pat (def \.xls$) en regex est dans sont fullname (avec path)
    srcdir (=cwd by default)
    dcond: condition sur la date. une fonction du type lambda d: d < ma date"""
    if srcdir is None:
        srcdir = os.getcwd()

    # get list of all files in dir
    fullnames = ["/".join([srcdir, f]) for f in os.listdir(srcdir)]

    # keep file (type f) and adjoinct a getmtime value
    fnames = [(f, op.getmtime(f)) for f in fullnames if fcond(f)]
    # print(fnames)
    # put this in a dataFrame
    colfilen = "fname"
    coldaten = "getmtime"
    df = DataFrame(fnames, columns=[colfilen, coldaten])

    # parse date col to_datetime format
    df.loc[:, coldaten] = to_datetime(df.loc[:, coldaten], unit="s")
    # filtering dates
    if dcond is not None:
        mask = df.loc[:, coldaten].apply(dcond)
        df = df.loc[mask, :]

    # sort
    df = df.sort_values(coldaten)[::-1]

    res = None

    try:
        if coln == "all":
            res = df.iloc[rang].loc[:]
        else:
            res = df.iloc[rang].loc[coln]

    except IndexError:
        logging.warning(f"srcdir={srcdir} et fcond={fcond}")
        logging.warning(f"rang={rang} et coln={coln} dans df(head)\n%s" % df.head(3))
        raise

    return res


def remove_ieme_xls_file(ieme=0, srcdir=None):
    """Supprime la ieme xls file sorted by name, if ieme = * les supprime toutes"""
    return remove_ieme_cond_ordered_files(
        ieme=ieme, srcdir=srcdir, cond=("extension", ext_is_xls)
    )


def ext_is_xls(ext):
    """takes an extention and say if it is xls"""
    return ext == ".xls"


def remove_ieme_cond_ordered_files(ieme, srcdir=None, cond=None, order=None):
    """cols can be: fullname, mtime, atime, ctime, size, isdir, isfile, islink, ext, path, bname. sorte by fullname by default, renvois les fichiers supprimés"""
    dff = get_df_files(srcdir)

    if order is None:
        sort_by, ascending = "fullname", True
    else:
        sort_by, ascending = order

    if cond is None:
        ffnames = dff.sort_values(by=sort_by, ascending=ascending).fullname
    else:
        ffnames = (
            dff.where(dff.loc[:, cond[0]].apply(cond[1]))
            .dropna()
            .sort_values(by=sort_by, ascending=ascending)
            .fullname
        )

    if ieme == "*":
        rfiles = []
        for ffname in ffnames:
            os.remove(ffname)
            rfiles.append(ffname)
            pass
        return rfiles
    else:
        os.remove(ffnames[ieme])
        return [ffnames[ieme]]


def remove_but_last_csv(srcdir=None):
    """remove all but most recent csv file"""
    if srcdir is None:
        cwd = os.getcwd()
        raise Exception(f"Vous devez fournir un srcdir. cwd={cwd}")

    dff = get_df_files(srcdir)
    ffnames = (
        dff.where(dff.loc[:, "extension"].apply(lambda ext: ext != ".csv"))
        .dropna()
        .fullname
    )
    rfiles = []
    for ffname in ffnames:
        os.remove(ffname)
        rfiles.append(ffname)

    ffnames = get_df_files(srcdir).sort_values(by="mtime").fullname

    if len(ffnames) > 1:
        for ffname in ffnames[:-1]:
            os.remove(ffname)
            rfiles.append(ffname)

    return rfiles


def remove_but_last_5(srcdir=None):
    """remove all but most recent csv file"""
    if srcdir is None:
        cwd = os.getcwd()
        raise Exception(f"Vous devez fournir un srcdir. cwd={cwd}")

    # dff = get_df_files(srcdir)
    ffnames = get_df_files(srcdir).sort_values(by="mtime").fullname

    rfiles = list()
    n = len(ffnames)
    if n > 5:
        for ffname in ffnames[: n - 5]:
            os.remove(ffname)
            rfiles.append(ffname)

    return rfiles


def get_df_files(srcdir=None):
    """Renvois un df avec un max d'info sur les files du srcdir
cols can be: fullname, mtime, atime, ctime, size, isdir, isfile, islink, ext, path, bname"""

    if srcdir is None:
        srcdir = os.getcwd()

    fullname = ["/".join([srcdir, f]) for f in os.listdir(srcdir)]
    df = DataFrame(data=fullname, columns=["fullname"])

    INFOS = {
        "mtime": op.getmtime,
        "atime": op.getatime,
        "ctime": op.getctime,
        "size": op.getsize,
        "isdir": op.isdir,
        "isfile": op.isfile,
        "islink": op.islink,
        "extension": lambda f: op.splitext(f)[1],
        "path": lambda f: op.split(f)[0],
        "bname": lambda f: op.split(f)[1],
    }

    for col, func in INFOS.items():
        df.loc[:, col] = df.fullname.apply(func)
        if "time" in col:
            df.loc[:, col] = to_datetime(df.fullname.apply(func), unit="s")
            df.loc[:, f"r{col}"] = df.loc[:, col].apply(lambda d: d.round("1s"))

    return df


def get_extraction_date(ffname, offset=60):
    """
    Parse the ffname which should be an extraction file to get the date from it.

    return the date - offset (default 3600s)
    """
    # extracting the date from the name
    pat = r"_(?P<day>\d+)_(?P<month>\d+)_(?P<year>\d+)_(?P<hour>\d+)_(?P<minute>\d+)\..{3}$"
    # print(f'pat={pat}\n{ffname}')
    trouvailles = re.compile(pat).search(ffname)
    if trouvailles is None:
        # au cas ou le nom du fichier n'est pas bien formé.  on prend la date du jour.
        dpextract = dt.datetime.now()
        pass
    else:
        trouvailles = trouvailles.groupdict()
        # convertir en int et crée la date
        # to get date previous extraction (qui si généré par le système doit être en phase avec le contenu de la df)
        dpextract = dt.datetime(**{k: int(v) for (k, v) in trouvailles.items()})

    # On lui enlève une heure créer un chevauchement et ne pas rater de mise à jour
    # sachant qu'il y a peu de nouveaux dossiers dans l'heure qui suit les extractions
    return dpextract - dt.timedelta(seconds=offset)


def get_previous_extraction_date(srcdir="./Data"):
    """get the second to the most recent extraction file name from dir srcdir,
    parse the name to get the date from it and return the date"""
    # before most recent file name
    bnames = get_df_files(srcdir).bname.sort_values()
    n = len(bnames)
    if n > 1:
        bmrfn = bnames.iloc[-2]
    elif n == 1:
        bmrfn = bnames.iloc[0]
    else:
        return False

    return get_extraction_date(bmrfn)


def une_date_est_plus_jeune(serieDeDates, dateref):
    """compare les dates d'une série avec une date de référence et renvois vrai si l'une des dates de la série est plus jeunes (plus grande) que la date de référence"""
    # eventuellement si serieDeDate is pd.serie
    # (serieDeDates < dateref).any()
    return any([dateref < d for d in serieDeDates])


def can_be_date(dt):
    return not (dt in ["", None] or pd.isna(dt))


def datetime_conversion(df, datetimecols):
    for coln in datetimecols:
        df.loc[:, coln] = df.loc[:, coln].apply(datetime_conv)
    return df


def simpledate_conversion(df, puredatecols):
    for coln in puredatecols:
        df.loc[:, coln] = df.loc[:, coln].apply(date_conv)
    return df


# #### Date conversion ####
def date_conv(dn, gedci_date=False, day_first=True):
    """Soit dn une date au format string, vérifie des erreurs et renvois la date au format %Y-%d-%m
    -dans le cas de gedci_date=True, on a des dates avec day first et moins de condition sur l'année"""
    if gedci_date:
        pat = r"(?P<day>\d{2})(?:_)(?P<month>\d{2})(?:_)(?P<year>\d{4})(?:_)(?P<hour>\d{2})(?:_)(?P<minute>\d{2})"
    else:
        pat = r"(?P<year>\d{4})(?:-|/)(?P<month>\d{2})(?:/|-)(?P<day>\d{2})"
        pat2 = r"(?P<day>\d{2})(?:-|/)(?P<month>\d{2})(?:/|-)(?P<year>\d{4})"

    if can_be_date(dn):
        try:
            try:
                trouvailles = re.compile(pat).search(dn)
                if trouvailles is None:
                    trouvailles = re.compile(pat2).search(dn)  # could be none again
                trvD = trouvailles.groupdict()
                day, month, year = map(int, [trvD["day"], trvD["month"], trvD["year"]])

                if not day_first:
                    day, month = month, day

                if gedci_date:
                    hour, minute = map(int, trvD["hour"], trvD["minute"])
                    year = 1900 if (1930 > year) else year
                    hour = 0 if hour > 24 else hour
                    minute = 0 if minute > 60 else minute
                else:
                    # pour une date de naissance ok
                    if (1930 > year) or (year > (pd.Timestamp.now().year - 10)):
                        year = 1900

                    # mauvaise partique ce sont des erreurs ignore le format américain des dates
                    month = 1 if month > 12 else month
                    daysinmonth = pd.Timestamp(
                        year=year, month=month, day=1
                    ).daysinmonth
                    day = 1 if day > daysinmonth else day

                try:
                    if gedci_date:
                        return dt.datetime(
                            year=year, month=month, day=day, hour=hour, minute=minute
                        )
                    else:
                        return pd.Timestamp(year=year, month=month, day=day).strftime(
                            "%Y-%m-%d"
                        )

                except OutOfBoundsDatetime as obd:
                    logging.error(f"pb avec {dn} dans {obd}")
                    raise obd

            except AttributeError as ae:
                logging.debug(
                    f"Error {ae}:\n {dn}=dn, pat={pat}, can_be_date={can_be_date(dn)}, gedci_date={gedci_date}"
                )
                pass

        except Exception as e:
            logging.warning(f"Check {dn}")
            raise (e)

    return None


def datetime_conv(dt):
    """Soit une Datetime dt la converti en Y-m-d"""
    if can_be_date(dt):
        try:
            ts = pd.to_datetime(dt, dayfirst=True)
            return ts.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            logging.warning("dt of type %s = '%s'" % (type(dt), dt))
            raise
    return None


def dt_equal(dt1, dt2, resolution=1):
    """compare des date selon une resolution en secondes (default 1)
    peut recevoir des dates en string ou de divers format.  le convertis en avant comparaison
    """
    # tt1, tt2 = type(dt1), type(dt2)
    # logging.debug(f"type(dt1)={tt1}, type(dt2)={tt2}, dt1={dt1}, dt2={dt2}")
    if dt1 is None and dt2 is None:
        return True

    if can_be_date(dt1) and can_be_date(dt2):
        try:
            ts1 = pd.Timestamp(dt1).round("ms")
            ts2 = pd.Timestamp(dt2).round("ms")
            return abs(ts1 - ts2).total_seconds() < resolution
        except Exception as e:
            logging.info(f"ts1={ts1}, ts2={ts2} et dif {ts1 - ts2}")
            raise (e)

    return None
