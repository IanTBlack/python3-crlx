from datetime import datetime
from requests.compat import urljoin

from .core import CRLX, assign_latlon
from .qartod import (location_test,gross_range_test,spike_test,
                     rate_of_change_test,flat_line_test,multi_variate_test,attenuated_signal_test)

class MAPPER:
    class NAVIGATION:
        GPS = {'latitude':'latitude',
               'longitude':'longitude',
               'hdop':'hdop',
               'num_sats': 'siv',
               'gps_quality': 'gps_quality'}

    class FLOWTHROUGH:
        SBE_38 = {'p1': 'sea_surface_temperature'}
        PCO2_A = {'p1': 'pco2',
                  'p2': 'sea_water_temperature',
                  'p3': 'co2',
                  'p4': 'cell_temperature',
                  'p5': 'equilibrator_temperature',
                  'p6': 'flow_rate',
                  'p7': 'barometric_pressure'}
        SBE_45 = {'p1': 'sea_water_temperature',
                  'p2': 'sea_water_electrical_conductivity',
                  'p3': 'sea_water_practical_salinity',
                  'p4': 'speed_of_sound_in_sea_water'}

    class MET:
        WIND_FWD = {'p7':'true_wind_velocity_v',
                    'p11': 'relative_wind_direction',
                    'p3': 'relative_wind_speed',
                    'p6': 'true_wind_velocity_u',
                    'p4': 'true_wind_direction',
                    'p5': 'true_wind_speed'}

class SIKULIAQ_INFO:
    ABBREV: str = 'SKQ'
    API_URL: str =  "https://coriolix.sikuliaq.alaska.edu/api/"
    DECIMATE_URL: str = urljoin(API_URL, 'decimateData')
    SENSOR_URL: str = urljoin(API_URL, 'sensor')
    PARAMETER_URL: str = urljoin(API_URL,'parameter')
    GPS_URL_ACTIVE: str = urljoin(API_URL, 'gnss_gga_bow')
    FLOW_CTDB_SBE38_URL_ACTIVE: str = urljoin(API_URL, 'sensor_float_6')
    FLOW_WL_PCO2_A_URL_ACTIVE: str = urljoin(API_URL, 'sensor_float_17')
    FLOW_WL_SBE45_A_URL_ACTIVE: str = urljoin(API_URL,'sensor_float_2')
    MET_WIND_FWD_URL_ACTIVE: str = urljoin(API_URL,'sensor_mixed_1')

    # GPS_ACTIVE_URL: str = urljoin(API_URL, 'gnss_gga_bow')
    # INTAKE_THERMO_ACTIVE_URL: str = urljoin(API_URL, 'sensor_float_6')
    # INTAKE_THERMO_ARCHIVE_URL: str = urljoin(API_URL, 'sensor_float_6_archive')
    # TSG_PRIMARY_ACTIVE_URL: str = urljoin(API_URL, 'sensor_float_2')
    # TSG_PRIMARY_ARCHIVE_URL: str = urljoin(API_URL, 'sensor_float_2_archive')
    # TSG_SECONDARY_ACTIVE_URL: str = urljoin(API_URL, 'sensor_float_3')
    # TSG_SECONDARY_ARCHIVE_URL: str = urljoin(API_URL, 'sensor_float_3_archive')
    # PCO2_PRIMARY_ACTIVE_URL: str = urljoin(API_URL, 'sensor_float_17')
    # PCO2_PRIMARY_ARCHIVE_URL: str = urljoin(API_URL, 'sensor_float_17_archive')
    #
    #
    # GPS_ARCHIVE_URL: str = urljoin(API_URL, 'gnss_gga_bow_archive')

class SIKULIAQ(CRLX):
    def __init__(self, verbose: bool = False, verify: bool = False):
        super().__init__(verbose, verify)
        self.VESSEL = SIKULIAQ_INFO


    def get_gps(self, begin_datetime: datetime, end_datetime: datetime, request_buffer: int = 60):
        ds = self.get_data(self.VESSEL.GPS_URL_ACTIVE, begin_datetime, end_datetime, request_buffer= request_buffer)
        if ds is not None:
            ds = ds.sortby('time')
            MAP = MAPPER.NAVIGATION.GPS
            VARS_TO_KEEP = list(MAP.keys()) + ['sensor_id']
            ds = ds[VARS_TO_KEEP]
            ds = ds.rename(MAP)
            ds = ds[list(sorted(ds.data_vars))]

            ds['qartod_location'] = location_test(ds.latitude, ds.longitude)

        return ds

    def get_sbe38(self, begin_datetime: datetime, end_datetime: datetime, request_buffer: int = 60, max_gap: int = 5):
        ds = self.get_data(self.VESSEL.FLOW_CTDB_SBE38_URL_ACTIVE, begin_datetime, end_datetime, request_buffer)
        if ds is not None:
            ds = ds.sortby('time')
            MAP = MAPPER.FLOWTHROUGH.SBE_38
            VARS_TO_KEEP = list(MAP.keys()) + ['sensor_id']
            ds = ds[VARS_TO_KEEP]
            ds = ds.rename(MAP)
            ds = ds[list(sorted(ds.data_vars))]
            gps_ds = self.get_gps(begin_datetime, end_datetime,request_buffer = request_buffer + 60)
            ds = assign_latlon(ds, gps_ds, max_gap)

            ds['qartod_location'] = location_test(ds.latitude, ds.longitude)
            ds['qartod_range_sea_surface_temperature'] = gross_range_test(ds.sea_surface_temperature,
                                                                            sensor_min = -5, sensor_max = 35,
                                                                          operator_min = -1, operator_max = 32)
            ds['qartod_spike_sea_surface_temperature'] = spike_test(ds.sea_surface_temperature)
            ds['qartod_rate_of_change_sea_surface_temperature'] = rate_of_change_test(ds.sea_surface_temperature)
            ds['qartod_flat_line_sea_surface_temperature'] = flat_line_test(ds.sea_surface_temperature, 12,6, 0.0001)
            ds['qartod_attenuated_sea_surface_temperature'] = attenuated_signal_test(ds.sea_surface_temperature, 0.002, 0.001)

        return ds


    def get_sbe45(self, begin_datetime: datetime, end_datetime: datetime, request_buffer: int = 60, max_gap: int = 3):
        ds = self.get_data(self.VESSEL.FLOW_WL_SBE45_A_URL_ACTIVE, begin_datetime, end_datetime, request_buffer)
        if ds is not None:
            ds = ds.sortby('time')
            MAP = MAPPER.FLOWTHROUGH.SBE_45
            VARS_TO_KEEP = list(MAP.keys()) + ['sensor_id']
            ds = ds[VARS_TO_KEEP]
            ds = ds.rename(MAP)
            ds = ds[list(sorted(ds.data_vars))]
            gps_ds = self.get_gps(begin_datetime, end_datetime,request_buffer = request_buffer + 60)
            ds = assign_latlon(ds, gps_ds, max_gap)

            ds['qartod_location'] = location_test(ds.latitude, ds.longitude)


            ds['qartod_range_sea_water_temperature'] = gross_range_test(ds.sea_water_temperature,
                                                                            sensor_min = -5, sensor_max = 35,
                                                                          operator_min = 1, operator_max = 32)
            ds['qartod_spike_sea_water_temperature'] = spike_test(ds.sea_water_temperature)
            ds['qartod_rate_of_change_sea_water_temperature'] = rate_of_change_test(ds.sea_water_temperature)
            ds['qartod_flat_line_sea_water_temperature'] = flat_line_test(ds.sea_water_temperature, 12,6, 0.0002)
            ds['qartod_attenuated_sea_water_temperature'] = attenuated_signal_test(ds.sea_water_temperature, 0.002, 0.001)


            ds['qartod_range_sea_water_electrical_conductivity'] = gross_range_test(ds.sea_water_electrical_conductivity,
                                                                            sensor_min = 0, sensor_max = 7,
                                                                          operator_min = 2.6, operator_max = 6)
            ds['qartod_spike_sea_water_electrical_conductivity'] = spike_test(ds.sea_water_electrical_conductivity)
            ds['qartod_rate_of_change_sea_water_electrical_conductivity'] = rate_of_change_test(ds.sea_water_electrical_conductivity)
            ds['qartod_flat_line_sea_water_electrical_conductivity'] = flat_line_test(ds.sea_water_electrical_conductivity, 12,6, 0.0003)
            ds['qartod_attenuated_sea_water_electrical_conductivity'] = attenuated_signal_test(ds.sea_water_electrical_conductivity, 0.002, 0.001)


            ds['qartod_range_sea_water_practical_salinity'] = gross_range_test(ds.sea_water_practical_salinity,
                                                                            sensor_min = 2, sensor_max = 42,
                                                                          operator_min = 20, operator_max = 36)
            ds['qartod_spike_sea_water_practical_salinity'] = spike_test(ds.sea_water_practical_salinity)
            ds['qartod_rate_of_change_sea_water_practical_salinity'] = rate_of_change_test(ds.sea_water_practical_salinity)
            ds['qartod_flat_line_sea_water_practical_salinity'] = flat_line_test(ds.sea_water_practical_salinity, 12,6, 0.003)
            ds['qartod_attenuated_sea_water_practical_salinity'] = attenuated_signal_test(ds.sea_water_practical_salinity, 0.004, 0.002)

        return ds

    def get_pco2_ldeo(self, begin_datetime: datetime, end_datetime: datetime, request_buffer: int = 60 * 60, max_gap: int = 3):
        ds = self.get_data(self.VESSEL.DECIMATE_URL, begin_datetime, end_datetime, request_buffer, model = 'SensorFloat17')
        if ds is not None:
            ds = ds.sortby('time')
            MAP = MAPPER.FLOWTHROUGH.PCO2_A
            VARS_TO_KEEP = list(MAP.keys()) + ['sensor_id']
            ds = ds[VARS_TO_KEEP]
            ds = ds.rename(MAP)
            ds = ds[list(sorted(ds.data_vars))]
            gps_ds = self.get_gps(begin_datetime, end_datetime,request_buffer = request_buffer + 900)
            ds = assign_latlon(ds, gps_ds, max_gap)

            ds['qartod_location'] = location_test(ds.latitude, ds.longitude)

            ds['qartod_range_pco2'] = gross_range_test(ds.pco2, sensor_min = 0, sensor_max = 1800,
                                                       operator_min = 110, operator_max = 504)
            ds['qartod_spike_pco2'] = spike_test(ds.pco2)
            ds['qartod_rate_of_change_pco2'] = rate_of_change_test(ds.pco2)
            ds['qartod_flat_line_pco2'] = flat_line_test(ds.pco2, 12,6, 1)
            ds['qartod_attenuated_pco2'] = attenuated_signal_test(ds.pco2, 2, 1)


            ds['qartod_range_sea_water_temperature'] = gross_range_test(ds.sea_water_temperature,
                                                                            sensor_min = -5, sensor_max = 35,
                                                                          operator_min = -1, operator_max = 32)
            ds['qartod_spike_sea_water_temperature'] = spike_test(ds.sea_water_temperature)
            ds['qartod_rate_of_change_sea_water_temperature'] = rate_of_change_test(ds.sea_water_temperature)
            ds['qartod_flat_line_sea_water_temperature'] = flat_line_test(ds.sea_water_temperature, 12,6, 0.0001)
            ds['qartod_attenuated_sea_water_temperature'] = attenuated_signal_test(ds.sea_water_temperature, 0.002, 0.001)


        return ds



    def get_pco2_apollo(self, begin_datetime: datetime, end_datetime: datetime, request_buffer: int = 60 * 60, max_gap: int = 3):
        ds = self.get_data(self.VESSEL.DECIMATE_URL, begin_datetime, end_datetime, request_buffer, model = 'SensorMixLg11')
        if ds is not None:
            ds = ds.sortby('time')
            ds = ds.where(ds.sensor_id == ds.sensor_id.values[-1],drop = True)

            param_ds = self.get_parameter_metadata(self.VESSEL.PARAMETER_URL, ds.sensor_id.values[-1])
            MAP = dict(zip(param_ds.data_fieldname.values.tolist(),param_ds.short_name.values.tolist()))
            VARS_TO_KEEP = param_ds.short_name.values.tolist() + ['sensor_id']
            ds = ds.rename(MAP)
            ds = ds[VARS_TO_KEEP]
            ds = ds[list(sorted(ds.data_vars))]
            gps_ds = self.get_gps(begin_datetime, end_datetime,request_buffer = request_buffer + 900)
            ds = assign_latlon(ds, gps_ds, max_gap)

            # ds['qartod_location'] = location_test(ds.latitude, ds.longitude)
            #
            # ds['qartod_range_pco2'] = gross_range_test(ds.pco2, sensor_min = 0, sensor_max = 1800,
            #                                            operator_min = 110, operator_max = 504)
            # ds['qartod_spike_pco2'] = spike_test(ds.pco2)
            # ds['qartod_rate_of_change_pco2'] = rate_of_change_test(ds.pco2)
            # ds['qartod_flat_line_pco2'] = flat_line_test(ds.pco2, 12,6, 1)
            # ds['qartod_attenuated_pco2'] = attenuated_signal_test(ds.pco2, 2, 1)
            #
            #
            # ds['qartod_range_sea_water_temperature'] = gross_range_test(ds.sea_water_temperature,
            #                                                                 sensor_min = -5, sensor_max = 35,
            #                                                               operator_min = -1, operator_max = 32)
            # ds['qartod_spike_sea_water_temperature'] = spike_test(ds.sea_water_temperature)
            # ds['qartod_rate_of_change_sea_water_temperature'] = rate_of_change_test(ds.sea_water_temperature)
            # ds['qartod_flat_line_sea_water_temperature'] = flat_line_test(ds.sea_water_temperature, 12,6, 0.0001)
            # ds['qartod_attenuated_sea_water_temperature'] = attenuated_signal_test(ds.sea_water_temperature, 0.002, 0.001)


        return ds


    def get_wind(self, begin_datetime: datetime, end_datetime: datetime, request_buffer: int = 60, max_gap: int = 3):
        ds = self.get_data(self.VESSEL.MET_WIND_FWD_URL_ACTIVE, begin_datetime, end_datetime, request_buffer, model = 'SensorFloat17')
        if ds is not None:
            ds = ds.sortby('time')
            MAP = MAPPER.MET.WIND_FWD
            VARS_TO_KEEP = list(MAP.keys()) + ['sensor_id']
            ds = ds[VARS_TO_KEEP]
            ds = ds.rename(MAP)
            ds = ds[list(sorted(ds.data_vars))]
            gps_ds = self.get_gps(begin_datetime, end_datetime,request_buffer = request_buffer + 60)
            ds = assign_latlon(ds, gps_ds, max_gap)

            ds['qartod_location'] = location_test(ds.latitude, ds.longitude)


            ds['qartod_range_relative_wind_direction'] = gross_range_test(ds.relative_wind_direction, sensor_min = 0,
                                                                          sensor_max = 360)
            ds['qartod_spike_relative_wind_direction'] = spike_test(ds.relative_wind_direction)
            ds['qartod_rate_of_change_relative_wind_direction'] = rate_of_change_test(ds.relative_wind_direction)
            ds['qartod_flat_line_relative_wind_direction'] = flat_line_test(ds.relative_wind_direction, 12,6, 0.5)
            ds['qartod_attenuated_relative_wind_direction'] = attenuated_signal_test(ds.relative_wind_direction, 2, 1)


            ds['qartod_range_relative_wind_speed'] = gross_range_test(ds.relative_wind_direction, sensor_min = 0,
                                                                      sensor_max = 75)
            ds['qartod_spike_relative_wind_speed'] = spike_test(ds.relative_wind_speed)
            ds['qartod_rate_of_change_relative_wind_speed'] = rate_of_change_test(ds.relative_wind_speed)
            ds['qartod_flat_line_relative_wind_speed'] = flat_line_test(ds.relative_wind_speed, 12,6, 0.25)
            ds['qartod_attenuated_relative_wind_speed'] = attenuated_signal_test(ds.relative_wind_speed, 0.2, 0.1)


        return ds


