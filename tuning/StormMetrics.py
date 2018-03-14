import json
import requests
import os

class StormMetrics:

    def __init__(self, PORT="8888"):
        self.port = PORT
        url = "http://localhost:"+PORT+"/api/v1/topology/summary"
        response = requests.get(url)
        j = json.loads(response.content)
        self.topologyIds = []
        for i in range(len(j['topologies'])):
            self.topologyIds.append(j['topologies'][i]['id'])
        self.tid = self.topologyIds[0]
        print self.tid
        self.setTopology(self.tid)

    def setTopology(self, tid):
        url = 'http://localhost:'+self.port+'/api/v1/topology/'+tid
        response = requests.get(url)
        self.main_json = json.loads(response.content)
    
    def getTopologyIds(self):
        return self.topologyIds

    def getSpouts(self):
        spouts = []
        for i in range(len(self.main_json['spouts'])):
            spouts.append(self.main_json['spouts'][i]['spoutId'])

        return spouts

    def getBolts(self):
        bolts = []
        for i in range(len(self.main_json['bolts'])):
            bolts.append(self.main_json['bolts'][i]['boltId'])

        return bolts

    def getAllCapacity(self):
        bolts = self.getBolts()
        cap = dict()
        for i in range(len(bolts)):
            cap[bolts[i]] = self.getCapacity(i)
        return cap

    def getAllSpoutStats(self):
        spouts = self.getSpouts()
        stats = dict()
        stats['completeLatency'] = dict()
        stats['emitted'] = dict()
        
        for i in range(len(spouts)):
            stats['completeLatency'][spouts[i]] = self.getCompleteLatency(i)
            stats['emitted'][spouts[i]] = self.getSpoutEmitted(i)
        return stats 

    def getAllBoltStats(self):
        bolts = self.getBolts()
        stats = dict()
        stats['capacity'] = dict()
        stats['processLatency'] = dict()
        stats['executeLatency'] = dict()
        stats['emitted'] = dict()
        stats['executed'] = dict()

        for i in range(len(bolts)):
            stats['capacity'][bolts[i]] = self.getCapacity(i)
            stats['processLatency'][bolts[i]] = self.getProcessLatency(i)
            stats['executeLatency'][bolts[i]] = self.getExecuteLatency(i)
            stats['emitted'][bolts[i]] = self.getBoltEmitted(i)
            stats['executed'][bolts[i]] = self.getExecuted(i)
        return stats 

    def getCompleteLatency(self, spoutIndex):
        return self.main_json['spouts'][spoutIndex]['completeLatency']

    def getSpoutEmitted(self, spoutIndex):
        return self.main_json['spouts'][spoutIndex]['emitted']

    def getCapacity(self, boltIndex):
        return self.main_json['bolts'][boltIndex]['capacity']

    def getProcessLatency(self, boltIndex):
        return self.main_json['bolts'][boltIndex]['processLatency']

    def getExecuteLatency(self, boltIndex):
        return self.main_json['bolts'][boltIndex]['executeLatency']
    
    def getBoltEmitted(self, boltIndex):
        return self.main_json['bolts'][boltIndex]['emitted']

    def getExecuted(self, boltIndex):
        return self.main_json['bolts'][boltIndex]['executed']

    def getJson(self):
        return self.main_json
