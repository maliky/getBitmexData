# -*- coding: utf-8 -*-
"""Types used in the packages."""
from typing import Literal, Optional
from pandas import Timestamp

bucketT = Literal['1m', '5m', '1h', '1d']
oTimestampT = Optional[Timestamp]
symbolT = Literal['XBTUSD', 'ADAM20']
