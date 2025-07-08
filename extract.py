import os
import s3fs
import xarray as xr
import pandas as pd
import numpy as np
import zarr
import pyproj
import cftime
import datetime as dt
import calendar
from dask.diagnostics import ProgressBar

# user inputs
ensmem = '01' # '01', '04', '06' or '15'
lon=-1
lat=53
year=2023

# load dataset from cloud
# zarr v3 method
fs = s3fs.S3FileSystem(anon=True, asynchronous=True, endpoint_url="https://chess-scape-o.s3-ext.jc.rl.ac.uk")
zstore_tmax = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/tmax_" + ensmem + "_year100km.zarr")
zstore_tmin = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/tmin_" + ensmem + "_year100km.zarr")
zstore_rsds = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/rsds_" + ensmem + "_year100km.zarr")
zstore_sfcWind = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/sfcWind_" + ensmem + "_year100km.zarr")
zstore_pr = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/pr_" + ensmem + "_year100km.zarr")
zstore_psurf = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/psurf_" + ensmem + "_year100km.zarr")
zstore_huss = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/huss_" + ensmem + "_year100km.zarr")

ds_tmax = xr.open_zarr(zstore_tmax)
ds_tmin = xr.open_zarr(zstore_tmin)
ds_rsds = xr.open_zarr(zstore_rsds)
ds_sfcWind = xr.open_zarr(zstore_sfcWind)
ds_pr = xr.open_zarr(zstore_pr)
ds_psurf = xr.open_zarr(zstore_psurf)
ds_huss = xr.open_zarr(zstore_huss)

ds = xr.merge([ds_tmax, ds_tmin, ds_rsds, ds_sfcWind, ds_pr, ds_psurf, ds_huss])
ds = ds.set_coords(['lat','lon'])



# Convert lon/lat to OSGB coords
proj = pyproj.Transformer.from_crs(4326, 27700, always_xy=True)
try: # needed to get around occasional irregular failure of conversion on first attempt
    x,y = proj.transform(lon,lat, errcheck=True)
except pyproj.exceptions.ProjError:
    x,y = proj.transform(lon,lat, errcheck=True)


# select out nearest gridpoint & year
with ProgressBar():
    dspoint = ds.sel(x=x, y=y, method='nearest').compute()
dspoint_year = dspoint.sel(time=str(year))


# convert to gregorian calendar from 360day
dspoint_year_greg = dspoint_year.convert_calendar('gregorian', align_on='date', missing=np.nan)
dspoint_year_greg = dspoint_year_greg.interpolate_na(dim='time', fill_value="extrapolate")


# convert units
# W/m^2 --> J/m^2/s --> MJ/m^2/day
rsds = dspoint_year_greg['rsds'].values/1000000*86400

# K --> degC
tmax = dspoint_year_greg['tasmax'].values - 273.15
tmin = dspoint_year_greg['tasmin'].values - 273.15

# /1000 to get kPa from Pa. # Formula from bottom of webpage https://cran.r-project.org/web/packages/humidity/vignettes/humidity-measures.html
vp = (dspoint_year_greg['huss'].values * (dspoint_year_greg['psurf'].values/1000))/(0.622 + (0.378 * dspoint_year_greg['huss'].values))

# already in m/s
sfcWind = dspoint_year_greg['sfcWind'].values

# kg/m^2/s --> mm/day
pr = dspoint_year_greg['pr'].values * 86400


# read in CO2 data from cloud
CO2data = pd.read_csv('s3://chess-scape-co2files/CHESS-SCAPE_RCP85_' + enemem + '.csv', storage_options={'endpoint_url': "https://fdri-o.s3-ext.jc.rl.ac.uk", 'anon': True})
CO2data = CO2data.set_index("YEAR")


# combine data into a pandas dataframe for easy write-out to file
dfpoint_year_greg = pd.DataFrame(index = pd.Index(np.arange(1,len(dspoint_year_greg.time)+1), name='DOY'),
                    columns = ['RAD', 'MINTMP', 'MAXTMP', 'VP', 'WIND', 'RAIN', 'CO2'])
dfpoint_year_greg['RAD'] = rsds
dfpoint_year_greg['MINTMP'] = tmin
dfpoint_year_greg['MAXTMP'] = tmax
dfpoint_year_greg['VP'] = vp
dfpoint_year_greg['WIND'] = sfcWind
dfpoint_year_greg['RAIN'] = pr
dfpoint_year_greg['CO2'] = CO2data.loc[year].values[0]

# write out to csv file
dfpoint_year_greg.to_csv('chess-scape_' + str(lon) + '_' + str(lat) + '_' + ensmem + '_' + str(year) + '.csv')