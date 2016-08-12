#!/usr/bin/python2.7

import json
import sys
import random
import requests
import os


def storm_metrics(index):
	PORT = '8888'
        url = 'http://localhost:'+PORT+'/api/v1/topology/summary'
	response = requests.get(url)
	#print response.content

	result_json = json.loads(response.content)
	topology_id = ''
	for i in range(len(result_json['topologies'])):
		print result_json['topologies'][i]['id']
		topology_id = result_json['topologies'][i]['id']

	url = 'http://localhost:'+PORT+'/api/v1/topology/'+ topology_id
	response = requests.get(url)

	#print response.content

	# Get capacity of all bolts
	result_json = json.loads(response.content)

	if not os.path.exists("json_files"):
	        os.makedirs("json_files")

	with open('json_files/topology'+index+'.json', 'w') as outfile:
	    json.dump(result_json, outfile)

	for i in range(len(result_json['spouts'])):
		url = 'http://localhost:'+PORT+'/api/v1/topology/'+ topology_id + '/component/' + result_json['spouts'][i]['spoutId']
		response = requests.get(url)
		new_result_json = json.loads(response.content)
		with open('json_files/'+result_json['spouts'][i]['spoutId']+'_'+index+'.json', 'w') as outfile:
			json.dump(new_result_json, outfile)

	for i in range(len(result_json['bolts'])):
		url = 'http://localhost:'+PORT+'/api/v1/topology/'+ topology_id + '/component/' + result_json['bolts'][i]['boltId']
		response = requests.get(url)
		new_result_json = json.loads(response.content)
		with open('json_files/'+result_json['bolts'][i]['boltId']+'_'+index+'.json', 'w') as outfile:
			json.dump(new_result_json, outfile)
		print result_json['bolts'][i]['capacity']

def main():
	index = sys.argv[1]
	storm_metrics(index)
        
if __name__ == '__main__':
    main()

