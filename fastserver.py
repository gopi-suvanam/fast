# FAST Server

from flask import Flask, request
import json
from importlib import import_module
import restserver
import auth_check
import re
import traceback
import pandas as pd

exposed_modules=["operator","basic_arithmetic","module"]

app = Flask(__name__,template_folder=".")

@app.route('/rest/<path>',methods=["GET","POST"])
def rest_mc(path):
	uri="/rest/"+path
	method = request.method.lower()
	if(method=='get'):
		try:
			result=restserver.get_uri(uri)
			if(type(result)==pd.DataFrame):
				return result.to_json(orient="split")
			return json.dumps(result)
		except Exception,e:
			traceback.print_exc()
			return json.dumps({"status":str(e)}),500
	if(method=='post'):
		try:
			data=json.loads(request.data) if request.data is not None else request.form
			
			return json.dumps(restserver.post_uri(uri,data))
		except Exception,e:
			traceback.print_exc()
			return json.dumps({"status":"Server Error "+str(e)}),500

@app.route('/fast/<module>/<function>',methods=["POST"])
def fast(module,function):
	if(method=='post'):
		args=json.loads(request.data) if request.data is not None else request.form

		restserver.post_uri(args['to_uri'],result)
	else:
		raise NotImplementedError( "This method not implemented" )
@app.route('/lambda/<module>/<function>',methods=["GET","POST"])
def lambda_mc(module,function):
	try:
		####### Getting the function to evaluate
		if(module not in exposed_modules):
			return json.dumps({"message":"Module not available"}),404
		try:	
			mod = import_module(module)
			func=getattr(mod,function)
		except Exception,e:
			traceback.print_exc()
			return json.dumps({"message":str(e)}),404
		method = request.method.lower()
		
		###### Getting the arguments for passing to the function call
		data={}
		if(method=='get'):
			args = request.args
		elif(method=='post'):
			args=json.loads(request.data) if request.data is not None else request.form
		for arg in args:
			
			if re.match("{{(?P<resource>.+)}}",args[arg]):
				data[arg]=restserver.get_uri(re.match("{{(?P<resource>.+)}}",args[arg]).groups()[0])
			else:
				if(arg!="to_do"):
					data[arg]=args[arg]
		
				
		######## Applying the function on the data
		to_do=args["to_do"]
		if(to_do=="apply"):
			if(isinstance(data,list)):
				result=func(*data)
			if( isinstance(data,dict)):
				result=func(**data)
		elif(to_do=="map"):
			result=map(lambda x: func(*x) if isinstance(x,list) else func(**x),data)
		elif(to_do=="reduce"):
			result=reduce(func,data)
		elif(to_do=="filter"):
			result=filter(lambda x: func(*x) if isinstance(x,list) else func(**x),data)
		
		####### Returning the results to the client or posting the results to a URI
		if(method=='get'):
			if(type(result)==pd.DataFrame):
				return result.to_json(orient="split")
			return json.dumps(result)
		elif(method=='post'):
			return json.dumps(result)
		else:
			raise NotImplementedError( "This method not implemented" )
	except Exception,e:
		traceback.print_exc()
		return json.dumps({"message":str(e)}),500
if __name__ == '__main__':
	app.run()
	pass