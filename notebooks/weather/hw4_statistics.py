
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol 
from mrjob.protocol import PickleProtocol 
from mrjob.protocol import RawValueProtocol 
from sys import stderr
import random
import numpy as np
from numpy import *
import pandas as pd
import sys,copy,traceback
#import Statistics2 as stats

logfile=stderr

class s:
    """ compute the mean of matrices (have to be of same size) """
    def __init__(self,mat):
        self.reset(mat)
        
    def reset(self,mat):
        self.n=pd.DataFrame(data=zeros(shape(mat)))
        self.sum=pd.DataFrame(data=zeros(shape(mat)))
        
    def accum(self,value):
        """ Add a value to the statistics """

        if type(value)!=pd.core.frame.DataFrame:
            value=pd.DataFrame(data=value)
 
        if np.shape(value) != shape(self.sum):
            raise Exception('in s.accum: shape of value:'+str(shape(value))+\
                            ' is not equal to shape of sum:'+str(shape(self.sum)))
        self.sum+=value.fillna(0)
        self.n+=(1-isnan(value))

    def compute(self):
        """ Returns the counts and the means for each entry """
        self.mean = self.sum / self.n
        return (self.n,self.mean)

    def add(self,other):
        """ add two statistics """
        self.n += other.n
        self.sum += other.sum
        
    def to_lists(self):
        return {'n':self.n.values.tolist(),\
                'sum':self.sum.values.tolist()}

    def from_lists(self,D):
        self.n=pd.DataFrame(data=D['n'])
        self.sum=pd.DataFrame(data=D['sum'])

class VecStat:
    """ Compute first and second order statistics of vectors of a fixed size n """
    def __init__(self,n):
        self.n=n
        self.reset()
        # Create a vector of length n and a matrix of size nXn
 
    def reset(self):
        n=self.n
        self.V=s(zeros(n))
        self.Cov=s(zeros([n,n]))
        
    def accum(self,U):
        """ accumulate statistics:
        U: an numpy array holding one vector
        """
        #check the length of U
        if len(U) != self.n :
            error='in Statistics.secOrdStat.accum: length of V='+str(self.n)+' not equal to length of U='+str(U.n)+'/n'
            sys.stderr.write(error)
            raise StandardError, error
        #check if U has the correct type
        if type(U) != ndarray:
            error='in Statistics.secOrdStat.accum: type of U='+str(type(U))+', it should be numpy.ndarray'
            sys.stderr.write(error)
            raise StandardError, error
        else:
            #do the work
            self.V.accum(U)
            self.Cov.accum(outer(U,U))
            
    def compute(self,k=5):
        """
        Compute the statistics. k (default 5) is the number of eigenvalues that are kept
        """

        # Compute mean vector
        (countV,meanV)=self.V.compute()

        # Compute covariance matrix
        (countC,meanC)=self.Cov.compute()
        cov=meanC-outer(meanV,meanV)
        std=[cov.ix[i,i] for i in range(np.shape(self.Cov.sum)[0])]
        try:
            (eigvalues,eigvectors)=linalg.eig(cov)
            order=argsort(-abs(eigvalues))	# indexes of eigenvalues from largest to smallest
            eigvalues=eigvalues[order]		# order eigenvalues
            eigvectors=eigvectors[order]	# order eigenvectors
            eigvectors=eigvectors[1:k]		# keep only top k eigen-vectors
            for v in eigvectors:
                v=v[order]     # order the elements in each eigenvector

        except Exception,e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,limit=2, file=sys.stderr)
            
            eigvalues=None
            eigvectors=None
        return {'count':self.V.n,'mean':meanV,'std':std,'eigvalues':eigvalues,'eigvectors':eigvectors}
        
    def add(self, other):
        """ add the statistics of s into self """
        self.V.add(other.V)
        self.Cov.add(other.Cov)
        
    def to_lists(self):
        return {'V':self.V.to_lists(),
                'Cov':self.Cov.to_lists()}

    def from_lists(self,D):
        self.V.from_lists(D['V'])
        self.Cov.from_lists(D['Cov'])
        self.n=len(self.V.sum)
    

class MRStats(MRJob):
    INPUT_PROTOCOL = RawProtocol
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = RawValueProtocol 
    
    def mapper(self, key, value):
        logfile.write("mapper")
        #logfile.write(key[1:-1])
        #logfile.write(value[1:-1])
        vec = value[1:-1].split(",")
        tav = np.array([float(d) for d in vec[:len(vec)/2]])
        tdiff = np.array([float(d) for d in vec[len(vec)/2:]])
        yield (1, tav)
        yield (2, tdiff)
        
    def reducer_init(self):
        logfile.write("reducer init")
        self.stat = VecStat(365)
        
    def reducer(self, key, value):
        # first it'll be tav, then tdiff
        logfile.write("reducer")
        for v in value:
            self.stat.accum(v)
        result = self.stat.compute()
        mean = result["mean"].values
        var = result["std"]
        rms = np.sqrt(np.mean(var))
        # formated as first mean and then rms
        yield (None, ",".join([str(d[0]) for d in mean]))
        yield (None, rms)
    
if __name__ == '__main__':
    MRStats.run()