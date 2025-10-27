#!/bin/bash
#SBATCH -p high-mem
#SBATCH --account=fdri
#SBATCH --partition=standard
#SBATCH --qos=high
#SBATCH -o %J.out
#SBATCH -e %J.err
#SBATCH --time=03:50:00
#SBATCH --mem=500GB
#SBATCH --array=1-77

joblist=/home/users/mattjbr/chess-scape-extract/joblist.txt

xll=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID -F , '$1==ArrayTaskID {print $2}' $joblist)
yll=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID -F , '$1==ArrayTaskID {print $3}' $joblist)
xur=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID -F , '$1==ArrayTaskID {print $4}' $joblist)
yur=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID -F , '$1==ArrayTaskID {print $5}' $joblist)

export PATH=/home/users/mattjbr/miniconda3/bin:$PATH
source /home/users/mattjbr/miniconda3/bin/activate chess-scape-extract
/home/users/mattjbr/miniconda3/envs/chess-scape-extract/bin/ipython /gws/ssde/j25b/ceh_generic/matbro/chess-scape-for-LINGRA-N/chess-scape-extract/extract_grid.py -- --filepath "/badc/deposited2021/chess-scape/data/rcp26/01/daily/" --outpath "/work/scratch-nopw2/mattjbr/chess-scape-extract_rcp26" --ensmem "01" --xllcorner $xll --yllcorner $yll --xurcorner $xur --yurcorner $yur --startdate "1981-01-01" --enddate "2079-12-31"
