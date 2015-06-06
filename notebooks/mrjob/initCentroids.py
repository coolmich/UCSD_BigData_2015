
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol 
from sys import stderr
import random

#logfile=open('log','w')
logfile=stderr
class MRInitCentroids(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol   
    
    def configure_options(self):
        super(MRInitCentroids, self).configure_options()
        self.add_passthrough_option(
            '--k', type='int', help='Number of clusters')
        self.add_passthrough_option(
            '--l', type='int', help='num of lines to choose from')
    
    def mapper(self, _, line):
        if random.randint(0, self.options.l) < self.options.k*2:
            yield (None, line.split(",")[3:])
    
    def reducer(self, _, line):
        # output int array
        for l in line:
            #for i in range(len(l)):
                #l[i] = int(l[i]) if l[i] != "" else None
            yield (None, ",".join(l))
        
if __name__ == '__main__':
    MRInitCentroids.run()