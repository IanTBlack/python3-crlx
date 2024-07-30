from datetime import datetime, timezone, timedelta
import fsspec
import multiprocessing
import os
import re
import xarray as xr

ENCODING = {'time': {'units': 'nanoseconds since 1900-01-01'}}


hourly_dir = '/media/support/data/sikuliaq/hourly'
save_dir = '/media/support/data/sikuliaq/daily'

vessel = 'SKQ'
search_condition = '*FLOW*MAIN*SBE45-A*.nc'
system = 'FLOWTHROUGH'
position = 'MAIN-LAB'
sensor = 'SBE45-A'


def main():
    local = fsspec.filesystem('file')
    files = local.glob(os.path.join(hourly_dir,search_condition))
    if len(files) == 0:
        exit()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    bdt = yesterday.replace(tzinfo=None)
    edt = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=None)

    fstr = bdt.strftime('%Y%m%d')
    filename = f"{vessel}_{system}_{position}_{sensor}_{fstr}.nc"
    filepath = os.path.join(save_dir, filename)

    foi = []
    for file in files:
        pattern = r'(\d{8}T\d{6}Z)'
        fbdt, fedt = re.findall(pattern, file)
        fbdt = datetime.strptime(fbdt, '%Y%m%dT%H%M%SZ')
        fedt = datetime.strptime(fedt, '%Y%m%dT%H%M%SZ')
        if bdt <= fbdt <= edt:
            foi.append(file)
        elif bdt <= fedt <= edt:
            foi.append(file)
        elif fbdt <= bdt <= fedt:
            foi.append(file)
        elif fbdt <= edt <= fedt:
            foi.append(file)
    with multiprocessing.Pool(os.cpu_count()-1) as pool:
        ds_list = pool.map(xr.open_dataset, foi)
    ds = xr.combine_by_coords(ds_list, combine_attrs='drop_conflicts')
    qartod_vars = [v for v in ds.data_vars if 'qartod_' in v]
    for qv in qartod_vars:
        ds[qv] = ds[qv].astype('int8')
    ds.to_netcdf(filepath, engine = 'netcdf4', encoding = ENCODING)


if __name__ == "__main__":
    main()