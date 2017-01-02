import random
from itertools import permutations
lines = open('lat_tw.yaml').readlines()
for i in range(1,5):
    random.shuffle(lines)
    print lines
    open('lat'+str(i)+'_tw.yaml', 'w').writelines(lines)
