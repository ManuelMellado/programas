#!/bin/bash
#SBATCH --time=3-00:00:00
#SBATCH --ntasks=1
#SBATCH --nodes=3
#SBATCH --account=mmellado_serv
#SBATCH --partition=cccmd
#SBATCH -w cibeles2-300
#SBATCH -w cibeles2-327
#SBATCH -w cibeles2-330
#SBATCH --mail-type=FAIL,END
#SBATCH --mail-user=manuel.mellado@uam.es
module load anaconda/anaconda3

mpirum -np 32 ejecutable

srun python aviones2.py