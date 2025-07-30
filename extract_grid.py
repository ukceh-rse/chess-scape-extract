import os
import sys
import s3fs
import xarray as xr
import pandas as pd
import numpy as np
import zarr
import argparse
import logging
from dask.diagnostics import ProgressBar

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# user inputs
parser = argparse.ArgumentParser()
parser.add_argument('--ensmem', type=str, required=True)
parser.add_argument('--outpath', type=str, required=True)
parser.add_argument('--xllcorner', type=float, required=True)
parser.add_argument('--yllcorner', type=float, required=True)
parser.add_argument('--xurcorner', type=float, required=True)
parser.add_argument('--yurcorner', type=float, required=True)
parser.add_argument('--startdate', type=str, required=True)
parser.add_argument('--enddate', type=str, required=True)
args = parser.parse_args()

ensmem = args.ensmem # '01', '04', '06' or '15'
outpath = args.outpath
xllcorner = args.xllcorner
yllcorner = args.yllcorner
xurcorner = args.xurcorner
yurcorner = args.yurcorner
startdate = args.startdate
enddate = args.enddate
if not os.path.exists(outpath):
    os.makedirs(outpath)

# create time coords
dayindex = pd.date_range(startdate, enddate)
allyears = dayindex.year.values
allyears = [int(year) for year in allyears]
doys = dayindex.dayofyear.values
doys = [int(doy) for doy in doys]


# read in CO2 data from cloud
logging.info('Getting CO2 data from cloud')
CO2datayr = pd.read_csv('s3://chess-scape-co2files/CHESS-SCAPE_RCP85_' + ensmem + '.csv', 
                        storage_options={'endpoint_url': "https://fdri-o.s3-ext.jc.rl.ac.uk", 'anon': True})
CO2datayr = CO2datayr.set_index("YEAR")

# put onto daily index (from yearly)
logging.info("Converting to a daily coordinate (from yearly)")
dayindex = pd.date_range(startdate, enddate)
CO2_daily = pd.DataFrame(index = dayindex, 
                         columns = ['CO2'])
years = np.unique(allyears)
for year in years:
    CO2_daily.loc[str(year)] = CO2datayr.loc[year].values

# load CHESS-SCAPE dataset from cloud
# zarr v3 method
logging.info('Loading CHESS-SCAPE datasets')
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
logging.info("Extracting out chunk to RAM")
xslice = slice(xllcorner,xurcorner)
yslice = slice(yllcorner,yurcorner)
tslice = slice(startdate, enddate)
with ProgressBar():
    dschunk = ds.sel(x=xslice, y=yslice).compute()
    
# check there is some data there
logging.info("Checking NaNs...")
if np.all(dschunk['rsds'][0,:,:].values == -1E20):
    logging.info('No data present in this chunk, exiting...')
    sys.exit()

# convert to gregorian calendar from 360day
logging.info("Move onto gregorian calendar (from 360-day)")
dschunk_greg = dschunk.convert_calendar('gregorian', align_on='date', missing=np.nan)
dschunk_greg = dschunk_greg.interpolate_na(dim='time', fill_value="extrapolate")
dschunk_greg = dschunk_greg.sel(time=tslice)


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
logging.info("Extracting out each gridpoint to csv")
xs = dschunk_greg['x'].values
ys = dschunk_greg['y'].values
timelen = len(dschunk_greg['time'])
fileheading = "YEAR,DOY,RAD,MINTMP,MAXTMP,VP,WIND,RAIN,CO2"
for x in xs:
    for y in ys:
        logging.info("Extracting out " + str(x) + "," + str(y))
        dspoint = dschunk_greg.sel(x=x, y=y)
        if dspoint['rsds'][0].values <= -1E10:
            logging.info("No data in this coord: " + str(x) + "," + str(y))
            continue
        dataarray = np.zeros((timelen,9))
        dataarray[:,0] = allyears
        dataarray[:,1] = doys
        dataarray[:,2] = dspoint['rsds'].values
        dataarray[:,3] = dspoint['tasmin'].values
        dataarray[:,4] = dspoint['tasmax'].values
        dataarray[:,5] = dspoint['vp'].values
        dataarray[:,6] = dspoint['sfcWind'].values
        dataarray[:,7] = dspoint['pr'].values
        dataarray[:,8] = CO2_daily['CO2'].values
        
        fname = os.path.join(outpath, 'chess-scape_' + str(years[0]) + '-' + str(years[-1]) + '_' + str(ensmem) + '_' + str(x) + '_' + str(y) + '.csv')
        logging.info("Saving to " + fname)
        fmt_string = ['%.4d', '%1d', '%.5f', '%.2f', '%.2f', '%.5f', '%.2f', '%.5f', '%.4f']
        np.savetxt(fname, dataarray, fmt=fmt_string, delimiter=',', header=fileheading)
        logging.info("Saved")
