import numpy as np
import xarray as xr


def pco2_to_fco2(pco2: xr.DataArray, temperature: xr.DataArray, xco2: xr.DataArray or None = None, sea_water_pressure: xr.DataArray or float = 0.000, atmospheric_pressure: xr.DataArray or float = 1000.000 ):
    """
    Convert pco2 to fco2.

    This code was modified from the p2fCO2 function in SeaCarb. If xco2 is not supplied, the assumption is that pco2 is near xco2 and is used to calculate the fugacity coefficient.

    :param pco2:
    :param temperature:
    :param sea_water_pressure:
    :param atmospheric_pressure:
    :return:
    """

    if isinstance(sea_water_pressure, float):
        sea_water_pressure = xr.full_like(pco2, sea_water_pressure)
    if isinstance(atmospheric_pressure, float):
        atmospheric_pressure = xr.full_like(pco2, atmospheric_pressure)

    tempk = temperature + 273.15
    pwat = (sea_water_pressure/1000)/1.01325  # mbar to atm
    patm = (atmospheric_pressure/1000)/1.01325
    ptot = patm + pwat

    b = -1636.75 + 12.0408 * tempk - 0.0327957 * tempk**2 + 0.0000316528 * tempk**3
    delta = 57.7-0.118 * tempk
    if xco2 is None:
        xco2_approx = pco2
        xc2 = (1-xco2_approx*1e-6)**2
    else:
        xc2 = (1-xco2*1e-6)**2

    fugcoeff = np.exp(ptot * (b+2*xc2*delta)/(82.057*tempk))
    fco2 = pco2 * fugcoeff
    return fco2





#
# def compute_co2flux(pco2w: xr.DataArray, pco2a: xr.DataArray, u10: xr.DataArray, t: xr.DataArray, s:xr.DataArray) -> xr.DataArray:
#
#     if pco2a.time != pco2a.time != u10.time != t.time != s.time:
#         combo =
#
#
#
#
#     pco2a = pco2a / 1.0e6
#     pco2w = pco2w / 1.0e6
#     Sc = 2073.1 - (125.62 * t) + (3.6276 * t**2) - (0.043219 * t**3)
#     k = 0.27 * u10**2 * np.sqrt(660.0 / Sc)
#     k = k / (100.0 * 3600.0)
#     T = t + 273.15
#     T100 = T / 100
#     K0 = 1000 * np.exp(-58.0931 + (90.5069 * (100/T)) + (22.2940 * np.log(T100)) +
#                        s * (0.027766 - (0.025888 * T100) + (0.0050578 * T100**2)))
#
#     flux = k * K0 * (pco2w - pco2a)
#     return flux