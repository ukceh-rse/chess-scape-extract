# README

A couple of scripts for extracting point timeseries data from the [CHESS-SCAPE dataset](https://catalogue.ceda.ac.uk/uuid/8194b416cbee482b89e0dfbe17c5786c/) stored in [JASMIN's S3 object store](https://help.jasmin.ac.uk/docs/short-term-project-storage/using-the-jasmin-object-store/). The [CHESS-SCAPE dataset](https://catalogue.ceda.ac.uk/uuid/8194b416cbee482b89e0dfbe17c5786c/) consists of 4 ensemble members and 4 RCP warming scenarios at a daily, 1km resolution. The [extract_point.py](https://github.com/ukceh-rse/chess-scape-extract/blob/main/extract_point.py) script extracts out the nearest gridpoint for lon/lat coordinates specified, of the specified ensemble member for a specified year for RCP8.5 only. The [extract_grid.py](https://github.com/ukceh-rse/chess-scape-extract/blob/main/extract_grid.py) file is similar, but extracts out all the gridpoints within a specified bounding box and time-period into separate csv files per gridpoint. (**Note:** They only works for ensmem 01 for now). Data is linearly interpolated from a 360day calendar to a gregorian calendar using Xarray's [convert_calendar](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.convert_calendar.html) function. The [extract_point.py](https://github.com/ukceh-rse/chess-scape-extract/blob/main/extract_point.py) script outputs a single csv file YYYY_ENSMEM_LON_LAT.csv. The [extract_grid.py](https://github.com/ukceh-rse/chess-scape-extract/blob/main/extract_grid.py) file outputs a single csv file *per gridpoint* chess-scape_YYYY1-YYYY2_ENSMEM_X_Y.csv. All csv files are structured with rows representing days and columns for:
- YEAR: The calendar year ([extract_grid.py](https://github.com/ukceh-rse/chess-scape-extract/blob/main/extract_grid.py) only)
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

These scripts were designed with the R-version of the [LINGRA-N Grass Yield model](https://models.pps.wur.nl/r-version-lingra-model) in mind, but could easily be adapted for other use-cases.

## Installation

### Requirements
These scripts require a python environment with the following packages installed:
- [numpy](https://numpy.org/)
- [scipy](https://scipy.org/)
- [xarray](https://docs.xarray.dev/en/stable/)
- [pandas](https://pandas.pydata.org/) >= 2.0
- [zarr](https://zarr.dev/) >= 3.0
- [dask](https://www.dask.org/)
- [cftime](https://unidata.github.io/cftime/)
- [s3fs](https://github.com/fsspec/s3fs/)
- [pyproj](https://pyproj4.github.io/pyproj/stable/)

These can be installed using your python package manager of choice. E.g.:

#### [Anaconda](https://anaconda.org/)
```conda create --name chess-scape-extract-env -c conda-forge numpy scipy xarray pandas>=2 zarr>=3 dask cftime s3fs pyproj```

to create a new environment in which to run the scripts, or:

```conda install -c conda-forge numpy scipy xarray pandas>=2 zarr>=3 dask cftime s3fs pyproj```

to install into an existing environment, or:

```conda create --name chess-scape-extract-env -f conda-envfile.txt```

to install into a new environment using the conda environment file provided here.

#### [Pip](https://packaging.python.org/en/latest/tutorials/installing-packages/)

```python -m venv ~/chess-scape-extract-env```
```source ~/chess-scape-extract-env/bin/activate```

to create a new [virtual python environment](https://docs.python.org/3/library/venv.html), then

```pip install numpy scipy xarray pandas>=2 zarr>=3 dask cftime s3fs pyproj```

or

```pip install -r requirements.txt```

to install.

Now that you have an environment suitable for running the scripts, obtain a copy of them by downloading them from their respective file pages: [extract_point.py](https://github.com/ukceh-rse/chess-scape-extract/blob/main/extract_point.py), [extract_grid.py](https://github.com/ukceh-rse/chess-scape-extract/blob/main/extract_grid.py), or cloning the repository with 

```git clone git@github.com:ukceh-rse/chess-scape-extract.git```

## Running instructions

Once installed, the scripts can be run by navigating to the folder containing them and executing them as e.g.:

```python extract_point.py --lon -1 --lat 52 --year 2020 --ensmem 01```

```python extract_grid.py --ensmem 01 --outpath "path/to/output/folder" --xllcorner 0 --yllcorner 0 --xurcorner 10000 --yurcorner 10000 --startdate "1981-01-01"--enddate "2079-12-31"```

or 

```ipython extract_point.py -- --lon -1 --lat 52 --year 2020 --ensmem 01```

```ipython extract_grid.py -- --ensmem 01 --outpath "path/to/output/folder" --xllcorner 0 --yllcorner 0 --xurcorner 10000 --yurcorner 10000 --startdate "1981-01-01"--enddate "2079-12-31"```

if using ipython.

The four arguments required to be passed to extract_point.py are:
- ```--lon``` longitude coordinate of location to extract nearest gridpoint from
- ```--lat``` latitude coordinate of location to extract nearest gridpoint from
- ```--year``` year of data to extract (from 1980 to 2080)
- ```--ensmem``` which ensemble member of the CHESS-SCAPE dataset to use. Possible options are '01', '04', '06', '15'.

The six arguments required to be passed to extract_grid.py are:
- ```--ensmem``` which ensemble member of the CHESS-SCAPE dataset to use. Possible options are '01', '04', '06', '15'
- ```--outpath``` folder in which to put the output csv files
- ```--xllcorner``` x coordinate of the "lower left" corner of the bounding box within which all grid points will be extracted
- ```--yllcorner``` y coordinate of the "lower left" corner of the bounding box within which all grid points will be extracted
- ```--xurcorner``` x coordinate of the "upper right" corner of the bounding box within which all grid points will be extracted
- ```--yurcorner``` y coordinate of the "upper right" corner of the bounding box within which all grid points will be extracted
- ```--startdate``` start date (inclusive) of the period of data to extract in "YYYY-MM-DD"" format
- ```--enddate``` end date (inclusive) of the period of data to extract in "YYYY-MM-DD"" format

### Further instructions for [extract_grid.py](https://github.com/ukceh-rse/chess-scape-extract/blob/main/joblist.txt) when extracting large numbers of gridpoints
This script works by pulling out the requested block of data from the remote object store into memory before reformatting into csv files. The loading into memory means a single run of this script can consume a lot of memory resources if a large area and long time-span is requested. Therefore in these scenarios it is recommended to split the area into smaller regions and/or run each individual region in parallel on a HPC. 

A example batch-job submission script for doing this on a [SLURM](https://slurm.schedmd.com/documentation.html)-based HPC environment is provided as [template.sbatch](https://github.com/ukceh-rse/chess-scape-extract/blob/main/template.sbatch). It is set up for the [JASMIN LOTUS2 cluster](https://help.jasmin.ac.uk/docs/batch-computing/lotus-overview/), which requires certain options or '[SLURM Flags](https://slurm.schedmd.com/sbatch.html)' at the [top of the file](https://github.com/ukceh-rse/chess-scape-extract/blob/fc8e19114b0b419e960aca0744f64f5d538492d8/template.sbatch#L2) that might not be necessary or might need to be different for other systems. Hopefully the documentation for your HPC environment will provide information on what is required. 

The [template.sbatch](https://github.com/ukceh-rse/chess-scape-extract/blob/main/template.sbatch) file submits several jobs that each cover a small area of the total spatial extent of the dataset, using a 'Task ID' unique to each job to select out a row of the [joblist.txt](https://github.com/ukceh-rse/chess-scape-extract/blob/main/joblist.txt) file. Each row of the [joblist.txt](https://github.com/ukceh-rse/chess-scape-extract/blob/main/joblist.txt) file contains a set of arguments for the [extract_grid.py](https://github.com/ukceh-rse/chess-scape-extract/blob/main/extract_grid.py) script, essentially adjusting the xllcorner, yllcorner, xurcorner, yurcorner arguments to cover a different area for each job. 
