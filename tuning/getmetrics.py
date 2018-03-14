import json
from StormMetrics import StormMetrics

metrics = StormMetrics("8080")
# If you have multiple topologies running then you will have to select the
# topology by the index before calling the function to get metrics
#for example like 
#metrics.setTopology(0)

print metrics.getJson()
print metrics.getBolts()
print metrics.getAllCapacity()
print metrics.getAllBoltStats()
print metrics.getAllSpoutStats()
