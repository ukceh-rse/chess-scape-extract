#!/bin/bash
#SBATCH -p high-mem
#SBATCH --account=fdri
#SBATCH --partition=standard
#SBATCH --qos=short
#SBATCH -o %J.out
#SBATCH -e %J.err
#SBATCH --time=01:00:00
#SBATCH --mem=40GB
#SBATCH --array=1-77

joblist=/home/users/mattjbr/chess-scape-extract/joblist.txt

xll=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID -F , '$1==ArrayTaskID {print $2}' $joblist)
yll=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID -F , '$1==ArrayTaskID {print $3}' $joblist)
xur=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID -F , '$1==ArrayTaskID {print $4}' $joblist)
yur=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID -F , '$1==ArrayTaskID {print $5}' $joblist)

export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate chess-scape-extract
/home/users/mattjbr/miniconda3/envs/chess-scape-extract/bin/ipython /home/users/mattjbr/chess-scape-extract/extract_grid.py -- --ensmem "01" --outpath "/work/scratch-nopw2/mattjbr/chess-scape-extract" --xllcorner $xll --yllcorner $yll --xurcorner $xur --yurcorner $yur --startdate "1981-01-01" --enddate "2079-12-31"
