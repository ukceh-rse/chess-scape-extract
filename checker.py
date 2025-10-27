import logging
import argparse
import s3fs
import zarr
import os
import xarray as xr
from dask.diagnostics import ProgressBar

logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

# user inputs
parser = argparse.ArgumentParser()
parser.add_argument('--s3', action='store_true')
parser.add_argument('--filepath', type=str, required=False)
parser.add_argument('--ensmem', type=str, required=True)
parser.add_argument('--outpath', type=str, required=True)
parser.add_argument('--xllcorner', type=float, required=True)
parser.add_argument('--yllcorner', type=float, required=True)
parser.add_argument('--xurcorner', type=float, required=True)
parser.add_argument('--yurcorner', type=float, required=True)
args = parser.parse_args()

ensmem = args.ensmem # '01', '04', '06' or '15'
filepath = args.filepath
outpath = args.outpath
xllcorner = args.xllcorner
yllcorner = args.yllcorner
xurcorner = args.xurcorner
yurcorner = args.yurcorner
s3 = args.s3

logging.info('Loading CHESS-SCAPE dataset')
if s3:
    fs = s3fs.S3FileSystem(anon=True, asynchronous=True, 
                           endpoint_url="https://chess-scape-o.s3-ext.jc.rl.ac.uk")
    zstore_rsds = zarr.storage.FsspecStore(fs, path="ens" + ensmem + 
                                           "-year100kmchunk/rsds_" + ensmem + 
                                           "_year100km.zarr")
    ds_rsds = xr.open_zarr(zstore_rsds, consolidated=False)
else:
    ds_rsds = xr.open_mfdataset(os.path.join(filepath, 'rsds/*.nc'), parallel=True)


logging.info("Extracting out first timestep to RAM")
xslice = slice(xllcorner,xurcorner)
yslice = slice(yllcorner,yurcorner)
with ProgressBar():
    testchunk = ds_rsds.sel(x=xslice, y=yslice, time=ds_rsds.time[0]).compute()

xs = testchunk['x'].values
ys = testchunk['y'].values
missingcoords = []
for x in xs:
    for y in ys:
        if testchunk['rsds'].sel(x=x, y=y).values <= -1E10:
            logging.info("No data in this coord: " + str(x) + "," + str(y))
            continue
        else:
            fname = os.path.join(outpath, 'chess-scape_1981-2079_' + \
                                 str(ensmem) + '_' + str(x) + '_' + str(y) + \
                                     '.csv')
            logging.info('Checking for existence of ' + fname)
            if os.path.exists(fname):
                logging.info('File exists')
            else:
                logging.info('FILE MISSING')
                missingcoords.append([x,y])
                
logging.info('All missing coordinates: ')
logging.info(missingcoords)
                
            
            

