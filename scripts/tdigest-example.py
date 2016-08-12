from tdigest import TDigest
from numpy.random import random
import time

time1 = time.time()
digest = TDigest()
for x in range(20000):
    digest.update(random())
time2 = time.time()
print time2-time1
print(digest.percentile(15))
