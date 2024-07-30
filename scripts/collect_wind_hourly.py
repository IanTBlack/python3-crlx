from datetime import datetime, timezone, timedelta
import numpy as np
import os
import pandas as pd

from crlx.sikuliaq import SIKULIAQ
from crlx.core import ENCODING

save_dir = '/media/support/data/sikuliaq/hourly'

system = 'METEOROLOGICAL'
position = 'FWD-MAST'
sensor = 'WIND-GILL75'

def main():
    edt = (datetime.now(timezone.utc) - timedelta(hours = 1)).replace(minute = 59, second = 59, microsecond=999999)
    bdt = edt.replace(minute = 0, second = 0, microsecond = 0)

    crlx = SIKULIAQ()
    ds = crlx.get_wind(bdt, edt)
    sensor_ids = np.unique(ds.sensor_id)
    for sensor_id in sensor_ids:  #In the event of multiple sensors being recorded over the request timespan, split the saved data into multiple files.
        _ds = ds.where(ds.sensor_id == sensor_id)
        _ds.attrs['sensor_id'] = np.unique(_ds.sensor_id)[0]
        _ds = _ds.drop_vars(['sensor_id'], errors = 'ignore')
        _ds = _ds.sortby('time')
        _ds = _ds.sel(time=slice(bdt.replace(tzinfo=None), edt.replace(tzinfo=None)))
        _bdt_str = pd.to_datetime(_ds.time.min().values).strftime('%Y%m%dT%H%M%SZ')
        _edt_str = pd.to_datetime(_ds.time.max().values).strftime('%Y%m%dT%H%M%SZ')
        filename = f"{crlx.VESSEL.ABBREV}_{system}_{position}_{sensor}_{_bdt_str}-{_edt_str}.nc"
        filepath = os.path.join(save_dir, filename)
        _ds.to_netcdf(filepath, engine = 'netcdf4', encoding = ENCODING)


if __name__ == "__main__":
    main()