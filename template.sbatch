#!/bin/bash
#SBATCH -p high-mem
#SBATCH --account=fdri
#SBATCH --partition=standard
#SBATCH --qos=short
#SBATCH -o %J.out
#SBATCH -e %J.err
#SBATCH --time=01:00:00
#SBATCH --mem=40GB

export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate chess-scape-extract
/home/users/mattjbr/miniconda3/envs/chess-scape-extract/bin/ipython /home/users/mattjbr/chess-scape-extract/extract_grid.py -- --ensmem "01" --outpath "/work/scratch-nopw2/mattjbr/chess-scape-extract" --xllcorner x1 --yllcorner y1 --xurcorner x2 --yurcorner y2 --startdate "1981-01-01" --enddate "2079-12-31"