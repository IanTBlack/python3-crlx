from datetime import datetime, timezone, timedelta
import numpy as np
import os
import pandas as pd
import requests
import warnings
import xarray as xr


REQ_DT_FMT = '%Y-%m-%d %H:%M:%S'
_USER = os.getlogin()
ENCODING = {'time': {'units': 'nanoseconds since 1900-01-01'}}


def assign_latlon(ds: xr.Dataset, gps_ds: xr.Dataset, max_gap: int = 3) -> xr.Dataset:
    _ds = ds[['time']]
    sensor_ids = ','.join(np.unique(gps_ds.sensor_id).tolist())
    _gps = gps_ds[['latitude', 'longitude']]
    interp = xr.combine_by_coords([_ds, _gps]).interpolate_na(dim='time',
                                                              method='nearest',
                                                              max_gap=timedelta(seconds=max_gap))
    assigned_ds = xr.combine_by_coords([ds, interp.sel(time=ds.time)])
    assigned_ds = assigned_ds.sel(time = ds.time)
    assigned_ds = assigned_ds[list(sorted(assigned_ds.data_vars))]
    assigned_ds.latitude.attrs['sensor_id'] = sensor_ids
    assigned_ds.longitude.attrs['sensor_id'] = sensor_ids
    return assigned_ds


class CRLX():
    def __init__(self, verbose: bool = True):
        self.verbose = verbose


    def get_data(self, URL: str,
                 begin_datetime: datetime, end_datetime: datetime,
                 request_buffer: int = 60,
                 decimator: int = 1,
                 model: str or None = None):
        _bdt = begin_datetime - timedelta(seconds = request_buffer)
        _edt = end_datetime + timedelta(seconds = request_buffer)

        params = {'date0': _bdt.strftime(REQ_DT_FMT),
                  'date1': _edt.strftime(REQ_DT_FMT),
                  'date_after': _bdt.strftime(REQ_DT_FMT),
                  'date_before': _edt.strftime(REQ_DT_FMT),
                  'decfactr': decimator,
                  'format': 'json'}

        if model is not None:
            params['model'] = model

        response = requests.get(URL, params = params)

        if response.status_code == requests.codes.ok:
            data = response.json()
            if len(data) == 0 or data == []:
                if self.verbose is True:
                    warnings.warn(f'No data are available for request to {response.url}')
                return None
            else:
                df = pd.DataFrame(data)
                df['time'] = pd.to_datetime(df['datetime_corrected'], format = 'mixed').dt.tz_localize(None)
                df.index = df.time
                ds = df.to_xarray()
                ds['sensor_id'] = ds['sensor_id'].astype(str)
                return ds
        else:
            if self.verbose is True:
                warnings.warn(f"{response.status_code}: {response.reason}")
                return None

