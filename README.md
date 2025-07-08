# README

Simple script for extracting point timeseries data from the [CHESS-SCAPE dataset](https://catalogue.ceda.ac.uk/uuid/8194b416cbee482b89e0dfbe17c5786c/) stored in JASMIN's S3 object store. The CHESS-SCAPE dataset consists of 4 ensemble members and 4 RCP warming scenarios at a daily, 1km resolution. This script extracts out the nearest gridpoint for lon/lat coordinates specified, of the specified ensemble (**Note:** Only works for ensmem 01 for now) for a specified year for RCP8.5 only. Data is linearly interpolated from a 360day calendar to a gregorian calendar using Xarray's [convert_calendar](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.convert_calendar.html) function. The script outputs a csv file YYYY_ENSMEM_LON_LAT.csv with rows representing the days of the year and columns for:
- DOY: Day of year
- RAD: Total shortwave radiation in MJ/m^2/day
- MINTMP: Minimum temperature in degC
- MAXTMP: Maximum temperature in degC
- VP: Vapour pressure in kPa
- WIND: Surface wind speed in m/s
- RAIN: Total precipitation in mm/day
- CO2: CO2 concentration in ppmv according to the RCP8.5 pathway (note this only varies by calendar year, so will be the same for every day of a given calendar year).

The vapour pressure $e$ is derived from the specific humidity $q$ and surface air pressure $p$ using the following equation:

$$e\approx \frac{qp}{0.622 + 0.378q}$$

It was designed with the R-version of the [LINGRA-N Grass Yield model](https://models.pps.wur.nl/r-version-lingra-model) in mind, but could easily be adapted for other use-cases.

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

```conda create --name chess-scape-extract-env -f conda-envfile.txt```

to install into a new environment using the conda environment file provided here.

#### Pip

```python -m venv ~/chess-scape-extract-env```
```source ~/chess-scape-extract-env/bin/activate```

to create a new virtual python environment, then

```pip install numpy scipy xarray pandas>=2 zarr>=3 dask cftime s3fs pyproj```

or

```pip install -r requirements.txt```

to install.

Now that you have an environment suitable for running the script, obtain a copy of it by downloading the script directly from the [script file page](https://github.com/ukceh-rse/chess-scape-extract/blob/main/extract.py), or cloning the repository with 

```git clone git@github.com:ukceh-rse/chess-scape-extract.git```

## Running instructions

Once installed, the script can be run by navigating to the folder containing it and executing it as e.g.:

```python extract.py --lon -1 --lat 52 --year 2020 --ensmem 01```

or 

```ipython extract.py -- --lon -1 --lat 52 --year 2020 --ensmem 01```

if using ipython.

The four arguments required to be passed are:
- ```--lon``` longitude coordinate of location to extract nearest gridpoint from
- ```--lat``` latitude coordinate of location to extract nearest gridpoint from
- ```--year``` year of data to extract (from 1980 to 2080)
- ```--ensmem``` which ensemble member of the CHESS-SCAPE dataset to use. Possible options are '01', '04', '06', '15'.
