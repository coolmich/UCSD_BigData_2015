
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol 
from mrjob.protocol import JSONValueProtocol 
from sys import stderr
import pickle
import random
import sys
import json
import math

def dist(arr1, arr2):
    dist = 0
    for i in range(len(arr1)):
        if arr1[i] != None and arr2[i] != None:
            dist += (arr1[i] - arr2[i])**2
    return dist

def strToFltArr(arr):
    for i in range(len(arr)):
        try:
            arr[i] = float(arr[i])
        except:
            arr[i] = None
    return arr

logfile=stderr
class MRUpdateCentroids(MRJob):
    INPUT_PROTOCOL = RawValueProtocol
    #OUTPUT_PROTOCOL = RawValueProtocol
    
    def configure_options(self):
        super(MRUpdateCentroids, self).configure_options()
        self.add_passthrough_option(
            '--k', type='int', help='Number of clusters')
        self.add_file_option('--centroids')
        
    def mapper_init(self):
        #logfile.write("mapper init")
        f = open(self.options.centroids)
        self.centroids = []
        i = 0
        for line in f:
            if i == self.options.k:
                break
            self.centroids.append(strToFltArr(line.strip().split(",")))
            i += 1
        #logfile.write("----{} centroids received-----".format(len(self.centroids)))
        f.close()
        #yield None, self.centroids
    
    def mapper(self, _, line):
        #logfile.write("mapper")
        if not line.startswith("station"):
            line = strToFltArr(line.split(",")[3:])
            minDist = sys.maxint
            center = 0
            for i in range(len(self.centroids)):
                if dist(line, self.centroids[i]) < minDist:
                    minDist = dist(line, self.centroids[i])
                    center = i
            yield (center, line)
        else:
            return
    
    def reducer(self, center, lines):
        #logfile.write("reducer {}".format(center))
        centroid = [0]*365
        line = [l for l in lines]
        ADC = 0
        #logfile.write("now center for {}".format(center))
        for l in line:
            for i in range(len(l)):
                #logfile.write("======")
                #logfile.write(str(centroid[i]))
                #logfile.write("~~")
                #logfile.write(str(l[i]))
                centroid[i] += l[i] if l[i] != None else 0
        for i in range(len(centroid)):
            centroid[i] = centroid[i] * 1.0 / len(line)
        # calculate ADC
        for l in line:
            ADC += math.sqrt(dist(l, centroid))
        yield(ADC/len(line), ",".join([str(d) for d in centroid]))
        
if __name__ == '__main__':
    MRUpdateCentroids.run()