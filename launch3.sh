#!/bin/bash
#SBATCH --ntasks=3
#SBATCH --nodes=3
#SBATCH --account=mmellado_serv
#SBATCH --partition=cccmd
#SBATCH -w cibeles2-300
#SBATCH -w cibeles2-327
#SBATCH -w cibeles2-318
#SBATCH --cpus-per-task=32
#SBATCH --mem-per-cpu=MaxMemPerCPU
#SBATCH --mail-type=FAIL,END
#SBATCH --mail-user=manuel.mellado@uam.es
module load anaconda/anaconda3
module load openmpi/intel/4.0.2-intel2020

mpirun -np 12 aviones2.py

srun python aviones2.py
