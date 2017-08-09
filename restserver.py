# restserver

data_store={}
import json
import pandas as pd

######## These functions need to implemented for REST machine to work
def get_uri(uri):
	global data_store
	if(hash(uri) in data_store.keys()):
		return data_store[(hash(uri))]
	else:
		return "Resource not found",404
def post_uri(uri,data):
	global data_store
	data_store[(hash(uri))]=pd.DataFrame(data)
	return {"status":"success"}
	
#http://localhost:5000/lambda/module/group_by?df={{/rest/blah}}&by=a&agg_column=b&agg_func=sum&to_do=apply
#	
	