
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol 
from mrjob.protocol import PickleProtocol
from mrjob.protocol import RawValueProtocol 

from sys import stderr
import random
import numpy as np
import pickle

logfile=stderr

class MRProject(MRJob):
    INPUT_PROTOCOL = RawProtocol
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def configure_options(self):
        super(MRProject, self).configure_options()
        self.add_file_option('--unitVecs')
        
    def mapper_init(self):
        f = open(self.options.unitVecs, "rb")
        self.unitVecs = pickle.load(f)
    
    def mapper(self, key, value):
        value = np.array([float(d) for d in value[1:-1].split(",")])
        proj = [np.dot(a, value) for a in self.unitVecs]
        yield (random.randint(0, 1000), proj)
        
    def reducer(self, key, value):
        if key < 10:
            for v in value:
                yield (None, ",".join([str(d) for d in v]))
            
if __name__ == '__main__':
    MRProject.run()