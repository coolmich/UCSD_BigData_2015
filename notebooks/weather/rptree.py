
import numpy as np
import sys

class RPNode():
    def __init__(self):
        self.value = None
        self.left = None
        self.right = None
#         self.num = len(arr)
#         self.value = np.median(np.array([d[depth] for d in arr]))
#         if depth < 6:
#             self.left = RPNode(np.array([d for d in arr if d[depth] < self.value]), depth+1)
#             self.right = RPNode(np.array([d for d in arr if d[depth] > self.value]), depth+1)
#         else:
#             self.left = None
#             self.right = None
    def build(self, arr, depth=0):
        self.value = np.median(np.array([d[depth] for d in arr]))
        leftArr = np.array([d for d in arr if d[depth] < self.value])
        rightArr = np.array([d for d in arr if d[depth] > self.value])