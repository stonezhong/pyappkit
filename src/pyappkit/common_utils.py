#####################################################
# Common Utilities
#####################################################

from typing import Optional
from datetime import datetime, timedelta

DT_FORMAT               = "%Y-%m-%d %H:%M:%S.%f"

# Data Conversion tools
def dt2str(dt:Optional[datetime])->Optional[str]:
    return None if dt is None else dt.strftime(DT_FORMAT)

def td2num(td:Optional[timedelta])->Optional[float]:
    return None if td is None else td.total_seconds()

def str2dt(dt_str:Optional[str])->Optional[datetime]:
    return None if dt_str is None else datetime.strptime(dt_str, DT_FORMAT)

def num2td(td_num:Optional[float])->Optional[timedelta]:
    return None if td_num is None else timedelta(seconds=td_num)
