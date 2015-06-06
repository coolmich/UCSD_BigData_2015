
from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol 
from sys import stderr
import random
import numpy as np
import pandas as pd

#logfile=open('logging','w')
logfile=stderr

def strArrWithMissingToNPArr(arr):
    return np.array([int(d) if d != "" else np.nan for d in arr])

def withinUS(latitude, longitude):
    if latitude > 49 or latitude < 24.5 or longitude > -66 or longitude < -124:
        return False
    return True
    
class MRPreprocess(MRJob):
    INTERNAL_PROTOCOL = PickleProtocol
    
    def configure_options(self):
        super(MRPreprocess, self).configure_options()
        self.add_file_option('--stations')
        
    def mapper_init(self):
        self.stations = pd.read_pickle(self.options.stations)

    def mapper(self, _, line):
        if not line.startswith("station"):
            line = line.split(",")
            if line[1] == "TMAX" or line[1] == "TMIN":
                station = self.stations.loc[line[0]]
                if withinUS(station.latitude, station.longitude):
                    yield ((line[0], line[2]), (line[1], line[3:]))
    
    def reducer(self, key, value):
        lines = [l for l in value]
        if len(lines) == 2:
            if lines[0][0] == "TMAX":
                tmax = strArrWithMissingToNPArr(lines[0][1])
                tmin = strArrWithMissingToNPArr(lines[1][1])
            else:
                tmin = strArrWithMissingToNPArr(lines[0][1])
                tmax = strArrWithMissingToNPArr(lines[1][1])
            tav = 0.5*(tmax+tmin)
            tdiff = tmax - tmin
            if np.sum(np.isnan(tav)) <= 50:
                tav = [str(d) if not np.isnan(d) else "0" for d in tav]
                tdiff = [str(d) if not np.isnan(d) else "0" for d in tdiff]
                yield (",".join(key), ",".join(tav+tdiff))
                
if __name__ == '__main__':
    MRPreprocess.run()