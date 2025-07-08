# README

Simple script for extracting point timeseries data from the CHESS-SCAPE dataset. The CHESS-SCAPE dataset consists of 4 ensemble members and 4 RCP warming scenarios. This script will extract out the nearest gridpoint for lon/lat coordinates specified, of the specified ensemble for a specified year for RCP8.5 only. Data will be linearly interpolated from a 360day calendar to a gregorian calendar. The script will output a csv file YYYY_ENSMEM_LON_LAT.csv with rows representing the days of the year and columns for:
- RAD: Total shortwave radiation in MJ/m^2/day
- MINTMP: Minimum temperature in degC
- MAXTMP: Maximum temperature in degC
- VP: Vapour pressure in kPa
- WIND: Surface wind speed in m/s
- RAIN: Total precipitation in mm/day
- CO2: CO2 concentration according to the RCP8.5 pathway (note this only varies by calendar year, so will be the same for every day of a given calendar year). 

## Installation

### Requirements
The script requires a python environment with the following packages installed:
- numpy
- scipy
- xarray
- pandas >= 2.0
- zarr >= 3.0
- dask
- cftime
- s3fs
- pyproj

These can be installed using your python package manager of choice. E.g.:

#### Anaconda
```conda create --name chess-scape-extract-env -c conda-forge numpy scipy xarray pandas>=2 zarr>=3 dask cftime s3fs pyproj```

to create a new environment in which to run this script, or:

```conda install -c conda-forge numpy scipy xarray pandas>=2 zarr>=3 dask cftime s3fs pyproj```

to install into an existing environment, or:

```conda create --name chess-scape-extract-env -f envfile.txt```

to install into a new environment using the conda environment file provided here.

#### Pip

```pip install numpy scipy xarray pandas>=2 zarr>=3 dask cftime s3fs pyproj```

or

```pip install -r requirements.txt```

to install using the requirements file provided here.


## Running instructions

Once installed, the script can be run as e.g.:

```python extract.py --lon -1 --lat 52 --year 2020 --ensmem 01```

or 

```ipython extract.py -- --lon -1 --lat 52 --year 2020 --ensmem 01```

if using ipython.

The four arguments required to be passed are:
- ```--lon``` longitude coordinate of location to extract nearest gridpoint from
- ```--lat``` latitude coordinate of location to extract nearest gridpoint from
- ```--year``` year of data to extract (from 1980 to 2080)
- ```--ensmem``` which ensemble member of the CHESS-SCAPE dataset to use. Possible options are '01', '04', '06', '15'.
