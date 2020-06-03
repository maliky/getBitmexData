Get BitMex Data
===============

This is an utility to download and update Bitmex historical price data.
It can download several binSize ("1m", "5m" or coarser). By default you
can easily download data for XBTUSD and ADAM20 instruments.

Licence
-------

GNU GENERAL PUBLIC LICENSE Version 3, 29 June 2007

Installation
------------

.. code:: bash

   git clone git@github.com:maliky/getBitMEXData.git
   cd getBitMEXData

Or with pip

.. code:: bash

   pip install getBitMEXData

Usage
-----

Just run the getBitMEXData.py like this for exemple

::

   python getBitMEXData.py --startTime "2019-08-01" --live
   # or if you install with pip
   getBitMEXData --startTime "2020-06-01"  --live

Or update an existing file previously downloaded with getBitMEXData.py
(columns of csv file should be the same). It will get the time from the
last saved record in the file and the bin size from the file name and
continue from there.

::

   python update_date.py yourBitmexDataFile-1d.csv

Help and other options
~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   python -m  getBitMEXData.getBitMEXData -h

.. code:: bash

   usage: getBitMEXData.py [-h] [--fout FOUT] [--count COUNT] [--pause PAUSE]
                           [--binSize BINSIZE] [--startTime STARTTIME]
                           [--endTime ENDTIME] [--live] [--logLevel LOGLEVEL]
                           [--entryPoint ENTRYPOINT] [--symbol SYMBOL]

   An application to download bitmex's data with what ever resolution you need.

   optional arguments:
     -h, --help            show this help message and exit
     --fout FOUT, -f FOUT  base Name of the csv file where to save the results.
                           (default btxData-freq-lastrecorddate.csv)
     --count COUNT, -c COUNT
                           Max number each of records in requests (default 600)
     --pause PAUSE, -p PAUSE
                           Min time to wait between 2 requests (default 1.2). to
                           avoid overloading the server
     --binSize BINSIZE, -b BINSIZE
                           Bin size or type requested, or time resolution
                           (default 1d), can also be 1m, 5m, 1h.
     --startTime STARTTIME, -s STARTTIME
                           Time to start the data collection (default, oldest
                           available 2016-05-05 04:00:00 'UTC'). Check time zones
     --endTime ENDTIME, -e ENDTIME
                           Time to end the data collection (default, now - 1 unit
                           of chosen resolution)-05-05 04:00:00 'UTC'). Check TZ
     --live, -l            If present use LIVE keys to get the data else use the
                           test site.
     --logLevel LOGLEVEL, -L LOGLEVEL
                           set the log level
     --entryPoint ENTRYPOINT, -E ENTRYPOINT
                           Set the entry level. the path to append to the LIVE or
                           TEST url before the query
     --symbol SYMBOL, -S SYMBOL
                           Set the symbol for which to get historical data def.
                           XBTUSD. default start date may change depending on
                           symbol


