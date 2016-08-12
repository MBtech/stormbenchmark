import os
from process import tail_latency as tail
import sys

path, dirs, files = os.walk("metrics").next()
file_count = len(files)
spout_num = int(sys.argv[1])
percentile = int(sys.argv[2])
skip_intervals = int(sys.argv[3])
for i in range(0,file_count):
    tail(str(i),spout_num,percentile,skip_intervals)
