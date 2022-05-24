#!/bin/bash

#SBATCH -J czi_org
#SBATCH -p general
#SBATCH -o out_%j.log
#SBATCH -e err_%j.log
#SBATCH --mail-type=ALL
#SBATCH --mail-user=aagond@iu.edu
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=12
#SBATCH --time=07:00:00
#SBATCH --mem=200G

#module load python/3.9.5

source $HOME/miniconda3/bin/activate
srun python cell_summary.py
conda deactivate