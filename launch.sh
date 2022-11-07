#!/bin/bash
#SBATCH --time=3-00:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=5G
#SBATCH --mail-type=FAIL,END
#SBATCH --mail-user=manuel.mellado@uam.es
module load anaconda/anaconda3

srun python aviones.py
