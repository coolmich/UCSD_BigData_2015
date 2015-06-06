
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol 
from mrjob.protocol import JSONValueProtocol 
from sys import stderr
import random
import sys
from updateCentroids import MRUpdateCentroids
from initCentroids import MRInitCentroids
import numpy as np
import pickle
import uuid
import os

# Create unique output directory in the student's s3_bucket
def outPutToFile(job, runner, fileName, catchKey=0):
    f = open(fileName, "w")
    ADC = []
    for line in runner.stream_output():
        key, value = job.parse_output_line(line)
        if catchKey == 1:
            ADC.append(key)
        #print key, value
        f.write(value.strip()+'\n')
    f.close()
    return None if len(ADC) == 0 else np.mean(ADC)

if __name__ == '__main__':
    args = sys.argv[1:]
    output_dir = "/Users/Jacob/Desktop/255/" + str(uuid.uuid4())
    INIT_CENTROIDS = output_dir + "/centroids000"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    ADC = []
    #ADC_FILE = "/Users/Jacob/Desktop/255/ADC10-1.p"
    
    choose_centroids_job = MRInitCentroids(args=args)
    with choose_centroids_job.make_runner() as choose_centroids_runner:
        choose_centroids_runner.run()
        outPutToFile(choose_centroids_job, choose_centroids_runner, INIT_CENTROIDS)
        i = 1
        CENTROIDS_FILE = INIT_CENTROIDS
        while True:
            #print "Iteration #%i" % i
            update_centroids_job = MRUpdateCentroids(args=args + ['--centroids='+CENTROIDS_FILE])
            with update_centroids_job.make_runner() as update_centroids_runner:
                update_centroids_runner.run()
                CENTROIDS_FILE = INIT_CENTROIDS + str(i)
                ADC.append(outPutToFile(update_centroids_job, update_centroids_runner,CENTROIDS_FILE, 1))
            i += 1
            if i == 10: break
    #pickle.dump( ADC, open( ADC_FILE, "wb" ) )
    print ",".join([str(d) for d in ADC])