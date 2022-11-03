#!/bin/bash
#SBATCH --time=3-00:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=100
#SBATCH --mem-per-cpu=8G
#SBATCH --mail-type=FAIL,END
#SBATCH --mail-user=manuel.mellado@uam.es

srun python aviones.py