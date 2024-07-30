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


def match_nearest(da_list: list, max_gap: int = 3600):
    combo = xr.combine_by_coords(da_list, combine_attrs = 'drop_conflicts')
    nearest = combo.interpolate_na(dim='time', method='nearest', max_gap=timedelta(seconds=max_gap))
    return nearest


def assign_latlon(ds: xr.Dataset, gps_ds: xr.Dataset, max_gap: int = 3) -> xr.Dataset:
    _ds = ds[['time']]
    sensor_ids = ','.join(np.unique(gps_ds.sensor_id).tolist())
    _gps = gps_ds[['latitude', 'longitude']]
    nearest = xr.combine_by_coords([_ds, _gps]).interpolate_na(dim='time',
                                                               method='nearest',
                                                               max_gap=timedelta(seconds=max_gap))
    assigned_ds = xr.combine_by_coords([ds, nearest.sel(time=ds.time)])
    assigned_ds = assigned_ds.sel(time = ds.time)
    assigned_ds = assigned_ds[list(sorted(assigned_ds.data_vars))]
    assigned_ds.latitude.attrs['sensor_id'] = sensor_ids
    assigned_ds.longitude.attrs['sensor_id'] = sensor_ids
    return assigned_ds


class CRLX():
    def __init__(self, verbose: bool = True, verify: bool = False):
        self.verbose = verbose
        self.verify = verify


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

        response = requests.get(URL, params = params, verify = self.verify)

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


    def get_sensor_metadata(self, URL, sensor_id, enabled: bool = True):
        params = {'sensor_id': sensor_id, 'enabled': enabled}
        response = requests.get(URL, params = params, verify = self.verify)

        if response.status_code == requests.codes.ok:
            data = response.json()
            if len(data) == 0 or data == []:
                if self.verbose is True:
                    warnings.warn(f'No data are available for request to {response.url}')
                return None
            else:
                df = pd.DataFrame(data)

                cols_to_drop = ['warranty_end_date','cal_recommended_freq_months','signal_type','comm_type',
                                'serial_baud_rate','serial_parity','serial_stop_bits','serial_bytesize',
                                'serial_timeout_seconds','polled_serial_commands','mac_address','sensor_ip',
                                'sensor_ethernet_protocol',
                                'native_file_path','native_file_name','native_data_description','native_type',
                                'binary_format_string','message_bol','message_eol','sample_data_message',
                                'text_regex_format','processing_script','power_min_volts','power_ideal_volts',
                                'power_max_volts','power_current_type','current_min_milliamps','current_max_milliamps',
                                'current_reference_volts','flow_rate_max_mls','flow_rate_ideal_mls','depth_min_meters',
                                'depth_max_meters','temperature_min_celsius','temperature_max_celsius','lamp_life_days',
                                'additional_time_offset_seconds','purchase_date','acquire_date','acquire_condition',
                                'end_date','current_location','condition','condition_details','daq_id',
                                'daq_serial_port','daq_analog_port','modbus_unit','transmit_ip','transmit_port',
                                'retransmit_ip','retransmit_port','packet_rate_hz','packet_size_bytes','archive_native',
                                'transmit_native','transmit_rate_hz','logging_modes','maintenance_A_text',
                                'maintenance_A_freq_days','maintenance_A_precruise','maintenance_A_postcruise',
                                'maintenance_A_last_date','maintenance_A_due_date','maintenance_B_text',
                                'maintenance_B_freq_days','maintenance_B_precruise','maintenance_B_postcruise',
                                'maintenance_B_last_date','maintenance_B_due_date','maintenance_C_text',
                                'maintenance_C_freq_days','maintenance_C_precruise','maintenance_C_postcruise',
                                'maintenance_C_last_date','maintenance_C_due_date','sensor_uuid','replacement_date',
                                'time_source']
                df = df.drop(cols_to_drop,axis = 1, errors = 'ignore')
                ds = df.to_xarray()
                return ds



    def get_parameter_metadata(self, URL, sensor_id, enabled: bool = True):
        params = {'sensor_id': sensor_id, 'enabled': enabled}
        response = requests.get(URL, params = params, verify = self.verify)

        if response.status_code == requests.codes.ok:
            data = response.json()
            if len(data) == 0 or data == []:
                if self.verbose is True:
                    warnings.warn(f'No data are available for request to {response.url}')
                return None
            else:
                df = pd.DataFrame(data)
                #
                # cols_to_drop = ['warranty_end_date','cal_recommended_freq_months','signal_type','comm_type',
                #                 'serial_baud_rate','serial_parity','serial_stop_bits','serial_bytesize',
                #                 'serial_timeout_seconds','polled_serial_commands','mac_address','sensor_ip',
                #                 'sensor_ethernet_protocol',
                #                 'native_file_path','native_file_name','native_data_description','native_type',
                #                 'binary_format_string','message_bol','message_eol','sample_data_message',
                #                 'text_regex_format','processing_script','power_min_volts','power_ideal_volts',
                #                 'power_max_volts','power_current_type','current_min_milliamps','current_max_milliamps',
                #                 'current_reference_volts','flow_rate_max_mls','flow_rate_ideal_mls','depth_min_meters',
                #                 'depth_max_meters','temperature_min_celsius','temperature_max_celsius','lamp_life_days',
                #                 'additional_time_offset_seconds','purchase_date','acquire_date','acquire_condition',
                #                 'end_date','current_location','condition','condition_details','daq_id',
                #                 'daq_serial_port','daq_analog_port','modbus_unit','transmit_ip','transmit_port',
                #                 'retransmit_ip','retransmit_port','packet_rate_hz','packet_size_bytes','archive_native',
                #                 'transmit_native','transmit_rate_hz','logging_modes','maintenance_A_text',
                #                 'maintenance_A_freq_days','maintenance_A_precruise','maintenance_A_postcruise',
                #                 'maintenance_A_last_date','maintenance_A_due_date','maintenance_B_text',
                #                 'maintenance_B_freq_days','maintenance_B_precruise','maintenance_B_postcruise',
                #                 'maintenance_B_last_date','maintenance_B_due_date','maintenance_C_text',
                #                 'maintenance_C_freq_days','maintenance_C_precruise','maintenance_C_postcruise',
                #                 'maintenance_C_last_date','maintenance_C_due_date','sensor_uuid','replacement_date',
                #                 'time_source']
                #df = df.drop(cols_to_drop,axis = 1, errors = 'ignore')
                ds = df.to_xarray()
                return ds