import os
import sys
import s3fs
import xarray as xr
import pandas as pd
import numpy as np
import zarr
import pyproj
import cftime
import datetime as dt
import calendar
import argparse
import logging
from dask.diagnostics import ProgressBar

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# user inputs
parser = argparse.ArgumentParser()
parser.add_argument('--ensmem', type=str, required=True)
parser.add_argument('--outpath', type=str, required=True)
args = parser.parse_args()

ensmem = args.ensmem # '01', '04', '06' or '15'
outpath = args.outpath

# create time coords
dayindex = pd.date_range("1/1/1981", "31/12/2079")
years = dayindex.year.values
doys = dayindex.dayofyear.values


# read in CO2 data from cloud
logging.info('Getting CO2 data from cloud')
CO2datayr = pd.read_csv('s3://chess-scape-co2files/CHESS-SCAPE_RCP85_' + ensmem + '.csv', storage_options={'endpoint_url': "https://fdri-o.s3-ext.jc.rl.ac.uk", 'anon': True})
CO2datayr = CO2datayr.set_index("YEAR")

# put onto daily index (from yearly)
dayindex = pd.date_range("1/1/1981", "31/12/2079")
CO2_daily = pd.DataFrame(index = dayindex, 
                         columns = ['CO2'])
years = np.unique(dayindex.year.values)
for year in years:
    CO2_daily.loc[str(year)] = CO2datayr.loc[str(year)]

# load CHESS-SCAPE dataset from cloud
# zarr v3 method
logging.info('Loading cloud datasets')
fs = s3fs.S3FileSystem(anon=True, asynchronous=True, endpoint_url="https://chess-scape-o.s3-ext.jc.rl.ac.uk")
zstore_tmax = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/tmax_" + ensmem + "_year100km.zarr")
zstore_tmin = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/tmin_" + ensmem + "_year100km.zarr")
zstore_rsds = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/rsds_" + ensmem + "_year100km.zarr")
zstore_sfcWind = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/sfcWind_" + ensmem + "_year100km.zarr")
zstore_pr = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/pr_" + ensmem + "_year100km.zarr")
zstore_psurf = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/psurf_" + ensmem + "_year100km.zarr")
zstore_huss = zarr.storage.FsspecStore(fs, path="ens" + ensmem + "-year100kmchunk/huss_" + ensmem + "_year100km.zarr")

ds_tmax = xr.open_zarr(zstore_tmax, consolidated=False)
ds_tmin = xr.open_zarr(zstore_tmin, consolidated=False)
ds_rsds = xr.open_zarr(zstore_rsds, consolidated=False)
ds_sfcWind = xr.open_zarr(zstore_sfcWind, consolidated=False)
ds_pr = xr.open_zarr(zstore_pr, consolidated=False)
ds_psurf = xr.open_zarr(zstore_psurf, consolidated=False)
ds_huss = xr.open_zarr(zstore_huss, consolidated=False)

ds = xr.merge([ds_tmax, ds_tmin, ds_rsds, ds_sfcWind, ds_pr, ds_psurf, ds_huss])
ds = ds.set_coords(['lat','lon'])


# select out & load coord block into RAM
xslice = slice(200000,300000)
yslice = slice(400000,500000)
tslice = slice("1981-01-01", "2079-12-31")
with ProgressBar():
    dschunk = ds.sel(x=xslice, y=yslice, time=tslice).compute()
    
# check there is some data there
if np.all(np.isnan(dschunk)):
    logging.info('No data present in this chunk, exiting...')
    sys.exit()

# convert to gregorian calendar from 360day
dschunk_greg = dschunk.convert_calendar('gregorian', align_on='date', missing=np.nan)
dschunk_greg = dschunk_greg.interpolate_na(dim='time', fill_value="extrapolate")


# convert units
logging.info('Converting units')
# W/m^2 --> J/m^2/s --> MJ/m^2/day
dschunk_greg['rsds'] = dschunk_greg['rsds']/1000000*86400

# K --> degC
dschunk_greg['tasmax'] = dschunk_greg['tasmax'] - 273.15
dschunk_greg['tasmin'] = dschunk_greg['tasmin'] - 273.15

# /1000 to get kPa from Pa. # Formula from bottom of webpage https://cran.r-project.org/web/packages/humidity/vignettes/humidity-measures.html
dschunk_greg['vp'] = (dschunk_greg['huss'] * (dschunk_greg['psurf']/1000))/(0.622 + (0.378 * dschunk_greg['huss']))

# kg/m^2/s --> mm/day
dschunk_greg['pr'] = dschunk_greg['pr'] * 86400


# loop over coords in chunk
xs = dschunk_greg['x'].values
ys = dschunk_greg['y'].values
fileheading = ["DOY,RAD,MINTMP,MAXTMP,VP,WIND,RAIN,CO2"]
#for x in xs:
#    for y in ys:
x = xs[0]
y = ys[0]
timelen = len(dschunk_greg['time'])
dspoint = dschunk_greg.sel(x=x, y=y)        
dataarray = np.zeros((timelen,9))
dataarray[:,0] = years
dataarray[:,1] = doys
dataarray[:,2] = dspoint['rsds'].values
dataarray[:,3] = dspoint['tasmin'].values
dataarray[:,4] = dspoint['taxmax'].values
dataarray[:,5] = dspoint['vp'].values
dataarray[:,6] = dspoint['sfcWind'].values
dataarray[:,7] = dspoint['pr'].values
dataarray[:,8] = CO2_daily.values

fname = 'chess-scape_1981-2079_' + str(ensmem) + '_' + str(x) + '_' + str(y) + '.csv'
np.savetxt(fname, dataarray, fmt='%s', delimiter=',', header=fileheading)