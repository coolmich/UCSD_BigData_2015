
from numpy import *
import numpy as np
from random import random
import sys,copy,traceback
import pandas as pd

        
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol 
from mrjob.protocol import PickleProtocol
from mrjob.protocol import RawValueProtocol 

from sys import stderr
import pickle

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
 
        if shape(value) != shape(self.sum):
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
        std=[cov.ix[i,i] for i in range(shape(self.Cov.sum)[0])]
        try:
            (eigvalues,eigvectors)=linalg.eig(cov)
            order=argsort(-abs(eigvalues))	# indexes of eigenvalues from largest to smallest
            eigvectors = eigvectors.transpose()
            eigvalues=eigvalues[order]		# order eigenvalues
            eigvectors=eigvectors[order]	# order eigenvectors
            eigvectors=eigvectors[0:k]		# keep only top k eigen-vectors
#             for v in eigvectors:
#                 v=v[order]     # order the elements in each eigenvector

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
        

class RPNode():
    def __init__(self):
        self.value = None
        self.left = None
        self.right = None
    def build(self, arr, depth=0):
        self.value = np.median(np.array([d[depth] for d in arr]))
        leftArr = np.array([d for d in arr if d[depth] < self.value])
        rightArr = np.array([d for d in arr if d[depth] > self.value])
        return (leftArr, rightArr)
    def write(self, arr):
        arr.append(self.value)
        if self.left:
            self.left.write(arr)
            self.right.write(arr)
    def read(self, arr):
        self.value = arr[0]
        if len(arr) > 1:
            self.left = RPNode()
            self.left.read(arr[1:(len(arr)-1)/2+1])
            self.right = RPNode()
            self.right.read(arr[(len(arr)-1)/2+1:])

class RPTree():
    def __init__(self):
        self.root = None
    def build(self, arr, depth = 0):
        if depth == 7:
            return None
        node = RPNode()
        leftArr, rightArr = node.build(arr, depth)
        node.left = self.build(leftArr, depth+1)
        node.right = self.build(rightArr, depth+1)
        return node
    def write(self, arr):
        self.root.write(arr)
    def read(self, arr):
        self.root = RPNode()
        self.root.read(arr)
    def encode(self, datum):
        result = []
        curNode = self.root
        for d in datum:
            if d < curNode.value:
                result.append('0')
                curNode = curNode.left
            else:
                result.append('1')
                curNode = curNode.right
        return "".join(result)


logfile=stderr

class MRRPPCA(MRJob):
    INPUT_PROTOCOL = RawProtocol
    INTERNAL_PROTOCOL = PickleProtocol
    
    def configure_options(self):
        super(MRRPPCA, self).configure_options()
        self.add_file_option('--unitVecs')
        self.add_file_option('--rptree')
        self.add_passthrough_option(
            '--level', type='int', help='level of the tree')
    
    def mapper_init(self):
        # initialize tree
        logfile.write("mapper_init")
        medians = pickle.load(open(self.options.rptree, "rb"))
        self.rptree = RPTree()
        self.rptree.read(medians)
        self.unitVecs = pickle.load(open(self.options.unitVecs, "rb"))
        self.level = self.options.level
    
    def mapper(self, key, value):
        logfile.write("mapper")
        arr = np.array([float(d) for d in value[1:-1].split(",")])
        proj = np.dot(self.unitVecs, arr)
        code = self.rptree.encode(proj)
        #for i in range(len(code)+1):
        yield (code[:self.level], arr)
    
    def combiner_init(self):
        logfile.write("combiner init")
        self.stat = VecStat(730)
        
    def combiner(self, key, value):
        logfile.write("combiner")
        for v in value:
            self.stat.accum(v)
        logfile.write("combiner yield")
        yield (key, self.stat.to_lists())
        
    def reducer_init(self):
        logfile.write("reducer_init")
        self.stat = VecStat(730)
    
    def reducer(self, key, value):
        logfile.write("reducer with key {}\n".format(key))
        for v in value:
            loc = VecStat(730)
            loc.from_lists(v)
            self.stat.add(loc)
        logfile.write("reducer compute\n")
        result = self.stat.compute(10)
        logfile.write("reducer conversion\n")
        eigenValues = ",".join([str(d) for d in result['eigvalues']])
        eigenVecs = []
        for vec in result['eigvectors']:
            eigenVecs.append(",".join([str(d) for d in vec]))
        eigenVecs.append(eigenValues)
        logfile.write("reducer yield\n")
        yield (key, "|".join(eigenVecs))
    
if __name__ == '__main__':
    MRRPPCA.run()