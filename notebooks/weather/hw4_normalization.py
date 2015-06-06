
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol 
from mrjob.protocol import PickleProtocol 
from sys import stderr
import random
import numpy as np
import pandas as pd

logfile=stderr

class MRNormalize(MRJob):
    INPUT_PROTOCOL = RawProtocol
    INTERNAL_PROTOCOL = PickleProtocol
    
    def configure_options(self):
        super(MRNormalize, self).configure_options()
        self.add_file_option('--stats')
    
    def mapper_init(self):
        f = open(self.options.stats)
        index = 0
        for line in f:
            if index == 0:
                self.tavMean = np.array([float(d) for d in line.split(",")])
            elif index == 1:
                self.tavRms = float(line)
            elif index == 2:
                self.tdiffMean = np.array([float(d) for d in line.split(",")])
            else:
                self.tdiffRms = float(line)
            index += 1
    
    def mapper(self, key, value):
        key = key[1:-1]
        vec = value[1:-1].split(",")
        tav = np.array([float(d) for d in vec[:len(vec)/2]])
        tdiff = np.array([float(d) for d in vec[len(vec)/2:]])
        yield (random.random(), (key, (tav-self.tavMean)/self.tavRms, (tdiff-self.tdiffMean)/self.tdiffRms))
        
    def reducer(self, key, value):
        line = [v for v in value][0]
        tav = [str(d) for d in line[1]]
        tdiff = [str(d) for d in line[2]]
        yield (line[0], ",".join(tav+tdiff))
        
if __name__ == '__main__':
    MRNormalize.run()