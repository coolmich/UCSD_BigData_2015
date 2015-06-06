
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from sys import stderr
import random

#logfile=open('logging','w')
#logfile=stderr

def emptyEntriesMoreThan(arr, num):
    return sum([d == '' for d in arr ]) > num

class MRPreprocess(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def mapper(self, _, line):
        #logfile.write("mapper start")
        if line.startswith("station"):
            yield (0, line)
        else:
            arr = line.split(",")
            if arr[1] != "TMAX":
                yield (2, line)
            elif emptyEntriesMoreThan(arr, 50):
                yield (1, line)
            else:
                yield (3, line)
    '''
    def combiner(self, key, line):
        line = [l for l in line]
        #logfile.write("combiner start")
        if key == 3:
            self.log.write("======== {} valid lines\n".format(len(line)))
            for l in line:
                yield (random.random(), l)
        else:
            for l in line:
                yield(key, l)
    '''
    def reducer(self, key, lines):
        arr = [l for l in lines]
        if key == 0:        
            yield ('', arr[0])
        elif key == 1:
            yield ('',"a======== TMAX entries with more than 50 empty slots {}\n".format(len(arr)))
        elif key == 2:
            yield('',"b======== Non-TMAX entries {}\n".format(len(arr)))
        else:
            '''
            yield('', arr[0])
            '''
            yield('',"c======== valid entries {}\n".format(len(arr)))
        
        

if __name__ == '__main__':
    MRPreprocess.run()