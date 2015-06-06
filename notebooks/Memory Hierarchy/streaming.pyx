import sys
import numpy as np
import matplotlib.pyplot as plt
# %matplotlib inline

from time import time
import math
# import ipython_memory_usage as imu
# imu.start_watching_memory()
import os

num = 150000

def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start

## A co-routine that acts as a sink - collects the items it recieves as input into a global-scope array
@coroutine
def collector(O):
    while True:
        item=(yield)
        # print item
        O[(item[2]-1)%num] = item


@coroutine
def av_filter(target=None ,float alpha=0.0, int t=0, float s=0.0):
    cdef float a
    cdef float r
    while True:
        a=(yield) # wait here to be get something sent.
        if(alpha==0):  # regular average
            t+=1
            alpha_=1.0/t
            s=alpha_*a+(1-alpha_)*s
        else:  # Exponentially decaying average
            s=alpha*a+(1-alpha)*s
        r=a-s  # a: input, s/n: average, r: residual
        target.send((a,s,r))

@coroutine
def std_filter(target=None,float alpha=0.0, int n=0, float s=0.0):
    cdef float a
    cdef float av
    cdef float r
    while True:
        (a,av,r)=(yield) # wait here to be get something sent.
        n+=1
        if(alpha==0):  # regular average
            s+=r*r
            std=math.sqrt(s/n)
            #sr=r/std  # a: input, sqrt(s/n): std, sr: rescaled residual
            target.send((a,av,r,s,std))
        else:  # Exponentially decaying average
            s=alpha*(r*r)+(1-alpha)*s
            std=math.sqrt(s)
            #sr=r/std
            target.send((a,av,n,s,std))

@coroutine
def cov_filter(target, int n, float cov, float alpha):
    cdef float a1
    cdef float av1
    cdef float std1
    cdef float a2
    cdef float av2
    cdef float std2
    while True:
        # nth data, nth average, n-1th average, nth std
        (a1, av1, av_1, std1, a2, av2, av_2, std2)=(yield)
        n += 1
        if alpha == 0:
            alpha_ = 1.0/n
        else:
            alpha_ = alpha
        cov = (1-alpha_)*cov + a1*a2*alpha_ + (1-alpha_)*av_1*av_2 - av1*av2
        
        target.send((cov,cov/(std1*std2),n))







G1=[(0.0,0.0,0.0,0.0,0.0)] * num
G2=[(0.0,0.0,0.0,0.0,0.0)] * num
Gcov = [(0.0,0.0)] * num
alpha = 0.03

# calculate mean
# already = 0
f1=open('data1.bin','rb')
f2=open('data2.bin','rb')
total = 6000000
t = 0
while total > 0:
    a1=np.fromfile(f1,count=num)
    a2=np.fromfile(f2,count=num)
    if len(a1) == 0: 
        break
    # continue last chunck info
    average1 = G1[-1][1]
    ridSum1 = G1[-1][3]
    # G1 = []
       
    average2 = G2[-1][1]
    ridSum2 = G2[-1][3]
    # G2 = []
    
    # reinitialize according to last saved value
    Std1 = std_filter(collector(G1),alpha,t,ridSum1)
    Av1=av_filter(Std1,alpha, t, average1)
    
    Std2 = std_filter(collector(G2),alpha,t,ridSum2)
    Av2=av_filter(Std2,alpha, t, average2)
    
    for i in range(len(a1)):
        Av1.send(a1[i])
        Av2.send(a2[i])
    # calculate covariance
    prevCov = Gcov[-1][0]
    # print G1[:10]

    # Gcov = []
    Cov = cov_filter(collector(Gcov),t,prevCov,alpha)
    for i in range(3,len(G1)):
        t += 1
        Cov.send((G1[i][0], G1[i][1], G1[i-1][1],G1[i][4],G2[i][0],G2[i][1],G2[i-1][1],G2[i][4]))
    if total % 100000 == 0:
      plt.figure(total)
      plt.plot([x[1] for x in Gcov])
      plt.grid()
        #print 1
    #print Gcov[0][1]
    #print([x[1] for x in G2[2000:2050]])
    total -= num
print "done"