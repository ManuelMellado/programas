import math
import re
import matplotlib.pyplot as plt
import csv
import Cython
import ripser
import numpy as np
import persim
import tadasets
import multiprocessing as mp
import time
import threading, queue
import datetime
from datetime import timedelta
from threading import Thread
from ripser import ripser
from persim import plot_diagrams
import sklearn.metrics
import array as arr 
import os
import glob
import pandas as pd
import copy
from geopy import distance
import statistics
from sklearn.metrics.pairwise import pairwise_distances
from scipy.stats import shapiro 
from scipy.stats import kstest
from scipy.stats import anderson
from scipy.stats import gamma
from haversine import haversine
import numpy as np
from sklearn import datasets
from scipy.spatial.distance import cdist
import tadasets
from persim import plot_diagrams, bottleneck
path = r'islandia'
all_files = glob.glob(path + "/*.ALL_FT+")
all_files.sort()
liiniciales = []
lifinales=[]
for i in range(0,len(all_files)):
    file=open(all_files[i])
    data=csv.reader(file, delimiter=';')
    cosa=[]
    for row in data:
        cosa.append(row)
    cosa.remove(cosa[0])
    miarchivo=[]
    a=len(cosa)
    if i%2!=0:
        for j in range(0,a):
            miarchivo2=[]
            miarchivo2.append(cosa[j][2]+cosa[j][6])
            miarchivo2.append(cosa[j][85])
            miarchivo.append(miarchivo2)
        liiniciales.append(miarchivo)
    else:
        for j in range(0,a):
            miarchivo2=[]
            miarchivo2.append(cosa[j][2]+cosa[j][6])
            miarchivo2.append(cosa[j][113])
            miarchivo.append(miarchivo2)
        lifinales.append(miarchivo)
rowsdepurini=[]
rowsdepurfin=[]
for j in range(0,len(liiniciales)):
    rowsdepuradoinicial=[]
    rowsdepuradofinal=[]
    a=len(liiniciales[j])
    b=len(lifinales[j])
    if j==0:
        if a>=b:
            for i in range(0,b):
                c=lifinales[j][i][0]
                for p in range(0,len(liiniciales[j])):
                    d=liiniciales[j][p][0]
                    if d==c:
                        rowsdepuradoinicial.append(liiniciales[j][p])
                        rowsdepuradofinal.append(lifinales[j][i])
                        liiniciales[j].remove(liiniciales[j][p])
                        break
                for p in range(0,len(liiniciales[j+1])):
                    e=liiniciales[j+1][p][0]
                    if c==e:
                        rowsdepuradoinicial.append(liiniciales[j+1][p])
                        rowsdepuradofinal.append(lifinales[j][i])
                        liiniciales[j+1].remove(liiniciales[j+1][p])
                        break
            rowsdepurini.append(rowsdepuradoinicial)
            rowsdepurfin.append(rowsdepuradofinal)
        else:
            for i in range(0,a):
                c=liiniciales[j][i][0]
                for p in range(0,len(lifinales[j])):
                    d=lifinales[j][p][0]
                    if d==c:
                        rowsdepuradoinicial.append(liiniciales[j][i])
                        rowsdepuradofinal.append(lifinales[j][p])
                        lifinales[j].remove(lifinales[j][p])
                        break
                for p in range(0,len(lifinales[j+1])):
                    e=lifinales[j+1][p][0]
                    if c==e:
                        rowsdepuradoinicial.append(liiniciales[j][i])
                        rowsdepuradofinal.append(lifinales[j+1][p])
                        lifinales[j+1].remove(lifinales[j+1][p])
                        break
            rowsdepurini.append(rowsdepuradoinicial)
            rowsdepurfin.append(rowsdepuradofinal)
    elif j!=0 and j!=len(liiniciales)-1:
        if a>=b:
            for i in range(0,b):
                c=lifinales[j][i][0]
                for p in range(0,len(liiniciales[j])):
                    d=liiniciales[j][p][0]
                    if d==c:
                        rowsdepuradoinicial.append(liiniciales[j][p])
                        rowsdepuradofinal.append(lifinales[j][i])
                        liiniciales[j].remove(liiniciales[j][p])
                        break
                for p in range(0,len(liiniciales[j+1])):
                    e=liiniciales[j+1][p][0]
                    if c==e:
                        rowsdepuradoinicial.append(liiniciales[j+1][p])
                        rowsdepuradofinal.append(lifinales[j][i])
                        liiniciales[j+1].remove(liiniciales[j+1][p])
                        break
                for p in range(0,len(liiniciales[j-1])):
                    f=liiniciales[j-1][p][0]
                    if f==c:
                        rowsdepuradoinicial.append(liiniciales[j-1][p])
                        rowsdepuradofinal.append(lifinales[j][i])
                        liiniciales[j-1].remove(liiniciales[j-1][p])
                        break
            rowsdepurini.append(rowsdepuradoinicial)
            rowsdepurfin.append(rowsdepuradofinal)
        else:
            for i in range(0,a):
                c=liiniciales[j][i][0]
                for p in range(0,len(lifinales[j])):
                    d=lifinales[j][p][0]
                    if d==c:
                        rowsdepuradoinicial.append(liiniciales[j][i])
                        rowsdepuradofinal.append(lifinales[j][p])
                        lifinales[j].remove(lifinales[j][p])
                        break
                for p in range(0,len(lifinales[j+1])):
                    e=lifinales[j+1][p][0]
                    if c==e:
                        rowsdepuradoinicial.append(liiniciales[j][i])
                        rowsdepuradofinal.append(lifinales[j+1][p])
                        lifinales[j+1].remove(lifinales[j+1][p])
                        break
                for p in range(0,len(lifinales[j-1])):
                    f=lifinales[j-1][p][0]
                    if f==c:
                        rowsdepuradoinicial.append(liiniciales[j][i])
                        rowsdepuradofinal.append(lifinales[j-1][p])
                        lifinales[j-1].remove(lifinales[j-1][p])
                        break
            rowsdepurini.append(rowsdepuradoinicial)
            rowsdepurfin.append(rowsdepuradofinal)
    else:
        if a>=b:
            for i in range(0,b):
                c=lifinales[j][i][0]
                for p in range(0,len(liiniciales[j])):
                    d=liiniciales[j][p][0]
                    if d==c:
                        rowsdepuradoinicial.append(liiniciales[j][p])
                        rowsdepuradofinal.append(lifinales[j][i])
                        liiniciales[j].remove(liiniciales[j][p])
                        break
            rowsdepurini.append(rowsdepuradoinicial)
            rowsdepurfin.append(rowsdepuradofinal)
        else:
            for i in range(0,a):
                c=liiniciales[j][i][0]
                for p in range(0,len(lifinales[j])):
                    d=lifinales[j][p][0]
                    if d==c:
                        rowsdepuradoinicial.append(liiniciales[j][i])
                        rowsdepuradofinal.append(lifinales[j][p])
                        lifinales[j].remove(lifinales[j][p])
                        break
            rowsdepurini.append(rowsdepuradoinicial)
            rowsdepurfin.append(rowsdepuradofinal)
datosimportantesiniciallista=[]
datosimportantesfinallista=[]
tiemposinicialeslista=[]
listahorasiniciales=[]
listahorasfinales=[]
tiemposfinaleslista=[]
for j in range(0,len(rowsdepurini)):
  tiemposinicial=[]
  tiemposfinal=[]
  datosimportantesinicial=[]
  datosimportantesfinal=[]
  for i in range(0,len(rowsdepurfin[j])):
    año=rowsdepurfin[j][i][1][0:4]
    tiemposinicial.append(re.findall(f'{año}(.*?):',rowsdepurini[j][i][1]))
    tiemposfinal.append(re.findall(f'{año}(.*?):',rowsdepurfin[j][i][1]))
    datosimportantesinicial.append(re.findall(r':::(.*?)::',rowsdepurini[j][i][1]))
    datosimportantesfinal.append(re.findall(r':::(.*?)::',rowsdepurfin[j][i][1]))
  for k in range(0,len(tiemposinicial)):
    for p in range(0,len(tiemposinicial[k])):
      tiemposinicial[k][p]=año+tiemposinicial[k][p]
  for k in range(0,len(tiemposfinal)):
    for p in range(0,len(tiemposfinal[k])):
      tiemposfinal[k][p]=año+tiemposfinal[k][p]
  tiemposinicialeslista.append(tiemposinicial)
  tiemposfinaleslista.append(tiemposfinal)
  datosimportantesiniciallista.append(datosimportantesinicial)
  datosimportantesfinallista.append(datosimportantesfinal)
for i in range(0,len(tiemposinicialeslista)):
    for j in range(0,len(tiemposinicialeslista[i])):
        for p in tiemposinicialeslista[i][j]:
            if len(p)!=14:
                tiemposinicialeslista[i][j].remove(p)
for i in range(0,len(tiemposfinaleslista)):
    for j in range(0,len(tiemposfinaleslista[i])):
        for p in tiemposfinaleslista[i][j]:
            if len(p)!=14:
                tiemposfinaleslista[i][j].remove(p)
horasinicialeslista=[]
horasfinaleslista=[]
stupid_times=datetime.datetime(1,1,1,0,0,0)
for i in range(0,len(tiemposinicialeslista)):
    horasiniciales=[]
    horasfinales=[]
    for j in range(0,len(tiemposinicialeslista[i])):
        horasinicialvuelo=[]
        horasfinalvuelo=[]
        for p in range(0,len(tiemposinicialeslista[i][j])):
            tuplainicialefimera=[]
            tuplainicialefimera.append(tiemposinicialeslista[i][j][p][0:4])
            for k in range(2,7):
                tuplainicialefimera.append(tiemposinicialeslista[i][j][p][2*k:2*k+2])
            tiempoexactoinicial=datetime.datetime(int(tuplainicialefimera[0]),int(tuplainicialefimera[1]),int(tuplainicialefimera[2]),int(tuplainicialefimera[3]),int(tuplainicialefimera[4]),int(tuplainicialefimera[5]))
            horasinicialvuelo.append(timedelta.total_seconds(abs(tiempoexactoinicial-stupid_times))/3600)
        for p in range(0,len(tiemposfinaleslista[i][j])):
            tuplafinalefimera=[]
            tuplafinalefimera.append(tiemposfinaleslista[i][j][p][0:4])
            for k in range(2,7):
                tuplafinalefimera.append(tiemposfinaleslista[i][j][p][2*k:2*k+2])
            tiempoexactofinal=datetime.datetime(int(tuplafinalefimera[0]),int(tuplafinalefimera[1]),int(tuplafinalefimera[2]),int(tuplafinalefimera[3]),int(tuplafinalefimera[4]),int(tuplafinalefimera[5]))
            horasfinalvuelo.append(timedelta.total_seconds(abs(tiempoexactofinal-stupid_times))/3600)
        horasiniciales.append(horasinicialvuelo)
        horasfinales.append(horasfinalvuelo)
    horasinicialeslista.append(horasiniciales)
    horasfinaleslista.append(horasfinales)
coordenadasiniciallista=[]
for i in range(0,len(datosimportantesiniciallista)):
    coordenadasinicialest=[]
    for j in range(0,len(datosimportantesiniciallista[i])):
        coordenada=[]
        for k in range(0,len(datosimportantesiniciallista[i][j])):
            lista=[]
            norte=[]
            oeste=[]
            for p in range(0,3):
                if datosimportantesiniciallista[i][j][k][6]=='S':
                    norte.append(int(datosimportantesiniciallista[i][j][k][2*p:2*p+2])*(-1))
                else:
                    norte.append(int(datosimportantesiniciallista[i][j][k][2*p:2*p+2]))
            for p in range(4,7):
                if datosimportantesiniciallista[i][j][k][14]=='W':
                    oeste.append(int(datosimportantesiniciallista[i][j][k][2*p:2*p+2])*(-1))
                else:
                    oeste.append(int(datosimportantesiniciallista[i][j][k][2*p:2*p+2]))
            lista.append(math.radians(norte[0]+norte[1]/60+norte[2]/3600))
            lista.append(math.radians(oeste[0]+oeste[1]/60+oeste[2]/3600))
            coordenada.append(lista)
        coordenadasinicialest.append(coordenada)
    coordenadasiniciallista.append(coordenadasinicialest)
coordenadasfinallista=[]
for i in range(0,len(datosimportantesfinallista)):
    coordenadasfinalest=[]
    for j in range(0,len(datosimportantesfinallista[i])):
        coordenada=[]
        for k in range(0,len(datosimportantesfinallista[i][j])):
            lista=[]
            norte=[]
            oeste=[]
            for p in range(0,3):
                if datosimportantesfinallista[i][j][k][6]=='S':
                    norte.append(int(datosimportantesfinallista[i][j][k][2*p:2*p+2])*(-1))
                else:
                    norte.append(int(datosimportantesfinallista[i][j][k][2*p:2*p+2]))
            for p in range(4,7):
                if datosimportantesfinallista[i][j][k][14]=='W':
                    oeste.append(int(datosimportantesfinallista[i][j][k][2*p:2*p+2])*(-1))
                else:
                    oeste.append(int(datosimportantesfinallista[i][j][k][2*p:2*p+2]))
            lista.append(math.radians(norte[0]+norte[1]/60+norte[2]/3600))
            lista.append(math.radians(oeste[0]+oeste[1]/60+oeste[2]/3600))
            coordenada.append(lista)
        coordenadasfinalest.append(coordenada)
    coordenadasfinallista.append(coordenadasfinalest)
puntosiniciales=[]
puntosfinales=[]
tiemposiniciales=[]
tiemposfinales=[]
for p in coordenadasiniciallista:
    puntos=[]
    for i in p:
        for j in i:
            puntosgrados=[]
            puntosgrados.append(math.degrees(j[0]))
            puntosgrados.append(math.degrees(j[1]))
            puntos.append(puntosgrados)
    puntosiniciales.append(puntos)
for p in coordenadasfinallista:
    puntos=[]
    for i in p:
        for j in i:
            puntosgrados=[]
            puntosgrados.append(math.degrees(j[0]))
            puntosgrados.append(math.degrees(j[1]))
            puntos.append(puntosgrados)
    puntosfinales.append(puntos)
for i in horasinicialeslista:
    tiempos=[]
    for j in i:
        for k in j:
            tiempos.append(k)
    tiemposiniciales.append(tiempos)
for i in horasfinaleslista:
    tiempos=[]
    for j in i:
        for k in j:
            tiempos.append(k)
    tiemposfinales.append(tiempos)
def matricespara(dict,puntos,puntos2,nombre):
    dict[nombre]=cdist(puntos,puntos2,metric=haversine)
def matricesportrozos(k,tipodepuntos,p,dict2,nombre):
    q={}
    h=math.floor(len(tipodepuntos[p])/k)
    matrices=[]
    for i in range(0,h+1):
        threads=[]
        if i!=h:
            puntos=tipodepuntos[p][i*k:(1+i)*k]
            for j in range(0,h+1):
                if j!=h:
                    threads.append(Thread(target=matricespara,args=(q,puntos,tipodepuntos[p][j*k:(j+1)*k],f"{i},{j}")))
                    threads[-1].start()
                    #matrices.append(cdist(puntos,puntosfinales[0][j*k:(j+1)*k],metric=haversine))
                else:
                    threads.append(Thread(target=matricespara,args=(q,puntos,tipodepuntos[p][h*k:len(tipodepuntos[p])],f"{i},{j}")))
                    threads[-1].start()
                    #matrices.append(cdist(puntos,puntosfinales[0][h*k:len(puntosfinales[0])],metric=haversine))
        else:
            puntos=tipodepuntos[p][h*k:len(tipodepuntos[p])]
            for j in range(0,h+1):
                if j!=h:
                    threads.append(Thread(target=matricespara,args=(q,puntos,tipodepuntos[p][j*k:(j+1)*k],f"{i},{j}")))
                    threads[-1].start()
                    #matrices.append(cdist(puntos,puntosfinales[0][j*k:(j+1)*k],metric=haversine))
                else:
                    threads.append(Thread(target=matricespara,args=(q,puntos,tipodepuntos[p][h*k:len(tipodepuntos[p])],f"{i},{j}")))
                    threads[-1].start()
                    #matrices.append(cdist(puntos,puntosfinales[0][h*k:len(puntosfinales[0])],metric=haversine))
        for t in threads:
            t.join()
    for i in range(0,h+1):
        for j in range(0,h+1):
            matrices.append(q[f"{i},{j}"])
    unidas=[]
    for i in range(0,h+1):
        a=np.concatenate((matrices[i*(h+1):(i+1)*(h+1)]),axis=1)
        unidas.append(a)
    dict2[nombre]=np.concatenate((unidas[0:len(unidas)]),axis=0)
def matricestrayectorias(i,puntos,nombre,q):
    q[nombre]=cdist(puntos[i],puntos[i],metric=haversine)
def matricestiempos(i,tiempos,nombre,q):
    q[nombre]=sklearn.metrics.pairwise_distances(np.array(tiempos[i]).reshape(-1,1),np.array(tiempos[i]).reshape(-1,1),metric='l1')
def diagramas(matriz,nombre,q):
    q[nombre]=ripser(matriz,distance_matrix=True)['dgms'][1]
def distancias(i,q):
    t1=Thread(target=matricesportrozos,args=(1000,puntosiniciales,i,q,"A"))
    t2=Thread(target=matricestiempos,args=(i,tiemposiniciales,"A2",q))
    t3=Thread(target=matricesportrozos,args=(1000,puntosfinales,i,q,"B"))
    t4=Thread(target=matricestiempos,args=(i,tiemposfinales,"B2",q))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    A=q["A"]
    B=q["B"]
    A2=q["A2"]
    B2=q["B2"]
    AA=A+A2
    BB=B+B2
    t5=Thread(target=diagramas,args=(A,"DA",q))
    t6=Thread(target=diagramas,args=(B,"DB",q))
    t7=Thread(target=diagramas,args=(AA,"DAA",q))
    t8=Thread(target=diagramas,args=(BB,"DBB",q))
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t5.join()
    t6.join()
    t7.join()
    t8.join()
    diagrams1_1= q["DA"]
    diagrams1_2= q["DAA"]
    diagrams2_1= q["DB"]
    diagrams2_2= q["DBB"]
    return [persim.bottleneck(diagrams1_1, diagrams2_1),persim.wasserstein(diagrams1_1,diagrams2_1),persim.bottleneck(diagrams1_2,diagrams2_2),persim.wasserstein(diagrams1_2,diagrams2_2)]
q={}
distanciasM=[]
distanciasMT=[]
distanciasMW=[]
distanciasMTW=[]
pool=mp.Pool(64)
results=pool.starmap(distancias,[(i,q) for i in range(0,len(puntosiniciales))])
pool.close()
for i in results:
    distanciasM.append(i[0])
    distanciasMW.append(i[1])
    distanciasMT.append(i[2])
    distanciasMTW.append(i[3])
import pandas
df2 = pandas.DataFrame(data={"col1": distanciasM})
df3 = pandas.DataFrame(data={"col1": distanciasMT})
df4 = pandas.DataFrame(data={"col1": distanciasMW})
df5 = pandas.DataFrame(data={"col1": distanciasMTW})
df2.to_csv("./distanciasislandia1819M.csv",sep=',',index=False)
df3.to_csv("./distanciasislandia1819MT.csv",sep=',',index=False)
df3.to_csv("./distanciasislandia1819MW.csv",sep=',',index=False)
df4.to_cvs("./distanciasislandia1819MTW.csv",sep=',',index=False)
