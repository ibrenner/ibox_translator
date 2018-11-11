from flask import Flask, request,abort,jsonify
from flask_restful import Api, Resource, reqparse
import requests
from requests.auth import HTTPBasicAuth
import urllib3
urllib3.disable_warnings()
import arrow
from infinisdk import InfiniBox
from capacity import GB
import time
import random, string
import json
import subprocess
#https://flask-restful.readthedocs.io/en/0.3.5/quickstart.html



class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

app = Flask(__name__)
api = Api(app)


## To be replaced with the actual values
ibox = "192.168.0.30"
notify_dir = '/tmp/'
notify_log = notify_dir+ "notify.log"
notify_script = "./notify_rm.sh"
cred=('admin', '123456')
creds = HTTPBasicAuth('admin', '123456')

### InfiniSDK Par/t
#Creds=HTTPBasicAuth(cred)
system=InfiniBox(ibox,cred)
system.login()
pool=system.pools.to_list()[0]
###
# Constants
onegig = 1000000000
id_str="546c4f25-FFFF-FFFF-ab9c-34306c4"
service_id='d4a44b0a-e3c2-4fec-9a3c-1d2cb14328f9'
date_format='YYYY-MM-DD HH:mm:ss'
id_len=5
opts_pars = { 'volume_type': '' , 'iscsi_init': '', 'image_id': '', 'bootable': 0, 'zone_code': 0}
#iscsi_init=''
mandatory_pars = ['name','size']
vol_name_length=10


## Functions
generate_random_name=lambda length: ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))
new_size = lambda  size: size/1000/1000/1000 
def new_date(date):
	print "Date is {}".format(date)
	tsa=arrow.get(str(date)[:-3])
	return tsa.format(date_format)

def set_new_id(id):
	embedding_zeros=id_len-len(str(id))
	new_id=id_str+embedding_zeros*"0"+str(id)
	return new_id
def notify_rm(file):
	try:
		return subprocess.Popen([notify_script,file])
	except Exception as E:
		notify_log_file=open(notify_log,'w')
		notify.write("Failed to call notify, {}".fomrat(E))

def add_metadata(vol_json):
    ret_dict={}
    vol_id=vol_json['result']['id']
    vol_obj_list=system.volumes.find(id=vol_id).to_list()
    if vol_obj_list:
        vol_obj=vol_obj_list[0]
        metadata=vol_obj.get_all_metadata()
        for key in metadata.keys():
            ret_dict[key]=metadata[key]
    return ret_dict
	

def poolselect():
    url="http://{}/api/rest/pools".format(ibox)
    pools = requests.get(url=url,auth=creds)
    return pools.json()['result'][-1]

def get_vol_data(vol_data,vol_id):
    return_json={}
    return_json['volumes']={}
    return_json['volumes'].update(add_metadata(vol_data))
    #return_json['id'] = set_new_id(outp_json['result']['id'])
    return_json['volumes']['id'] = vol_id
    return_json['volumes']['size'] = new_size(int(vol_data['result']['size']))
    return_json['volumes']['create_at'] = new_date(int(vol_data['result']['created_at']))
    #return_json['volumes']['name'] = vol_data['result']['name']
    return_json['volumes']['lun_id'] = vol_data['result']['id']
    #return_json['volumes']['iscsi_init'] = vol_data['result']['serial']
    return_json['volumes']['iscsi_init'] = iscsi_init
    return_json['volumes']['service_id'] = service_id
    return_json['volumes']['status'] = 'available'
    if vol_data['result']['mapped']:
        return_json['volumes']['attach_status'] = 'online'
    else:
        return_json['volumes']['attach_status'] = 'offline'
    return return_json

class VolumesList(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('pool_id', type=int, required=False, location='json',default=poolselect()['id'])
        self.reqparse.add_argument('name', type=str, required=True, location='json')
        self.reqparse.add_argument('provtype', type=str, required=False, location='json', default='THIN')
        self.reqparse.add_argument('size', type=int, required=True, location='json')
        super(VolumesList, self).__init__()
    def get(self):
        #url="http://{}/api/rest/volumes/{}".format(ibox, id)
        url="http://{}/api/rest/volumes".format(ibox)
        #outp = requests.get(url=url,auth=HTTPBasicAuth('iscsi', '123456')).json()['result']
        outp = requests.get(url=url,auth=creds)
        return outp.json(), int(outp.status_code)
    def post(self):
        #url="http://{}/api/rest/volumes".format(ibox)
        #args = self.reqparse.parse_args()
        #vol = {
        #    'pool_id': args['pool_id'],
        #    'name': args['name'],
        #    'provtype': args['provtype'],
        #    'size': args['size']*onegig
        #}
        #outp = requests.post(url=url,json=vol, auth=creds)
        body=request.json

        #print "this is body {}".format(body)
        for mandatory_key in mandatory_pars:
            if mandatory_key not in body['volumes']: ##1
                print "mandatory_key {} can't be found".format(mandatory_key)
                raise InvalidUsage('Mandatory key cannot be found', status_code=410)
        try:
            #print "Creating a volume, size {}, name {}".format(body['size'],body['name'])
            new_name=generate_random_name(vol_name_length)
            #volume=system.volumes.create(pool=pool,size=body['size']*GB,name=body['name'])
            volume=system.volumes.create(pool=pool,size=body['volumes']['size']*GB,name=new_name)
        except Exception as E:
        
            raise InvalidUsage('Error Caught {}'.format(E), status_code=420)
        volume.set_metadata('name',body['volumes']['name'])
        for optional_key in opts_pars:
            if optional_key in body['volumes']:
                volume.set_metadata(optional_key,body['volumes'][optional_key])
            else:
                volume.set_metadata(optional_key,opts_pars[optional_key])
        vol_new_id=set_new_id(volume.get_id())
        notify=notify_dir+vol_new_id
        url="http://{}/api/rest/volumes/{}".format(ibox,volume.get_id())
        vol_infi_data=requests.get(url=url,auth=creds)
        #print "INFI DATA****** {}".format(vol_infi_data)
        global iscsi_init
        global volume_type
        if 'iscsi_init' in body['volumes'].keys():
            iscsi_init=body['volumes']['iscsi_init']
        else:
            iscsi_init=''
        #if 'volume_type' in body['volumes'].keys():
        #    volume_type=body['volumes']['volume_type']
        #else:
        #    volume_type=''
        vol_data=get_vol_data(vol_infi_data.json(), vol_new_id)
        notify_vol={}
        notify_vol={"snapshot_id":"", "notify_type":"volume_create","status":"available","result":"success"}
        notify_vol['volume_id'] = vol_new_id
        notify_vol['create_at'] = vol_data['volumes']['create_at']
        try:
            #print "notify is {}".format(notify)
            notify_f=open(notify,'w')
            notify_f.writelines(json.dumps(notify_vol))
            notify_f.close()
            notify_rm(notify)
        except Exception as E:
            str="Failed! {}".format(E)
        #print str
            return str,400
	vol_data['volumes']['status']='creating'
        return vol_data, 200

class Volume(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        #self.reqparse
    def get(self, vol_id):
        #url="http://{}/api/rest/volumes/{}".format(ibox, id)
        infi_id=vol_id[-5:]
        url="http://{}/api/rest/volumes/{}".format(ibox, infi_id)
        print "URL IS {}".format(url)
        outp = requests.get(url=url,auth=creds)
        outp_json = outp.json()
        if outp_json['error'] or not outp_json['result']:
            return {},'200'
        return_json=get_vol_data(outp_json,vol_id)
        #return outp.json() int(outp.status_code)
        return return_json, int(outp.status_code)
        
    def post(self, id):
       pass

    def put(self, id):
        body=request.json
	print type(body)
	print body.keys()
        string="id is {} data is {}".format(id, body)
        return string,400
    def delete(self, vol_id):
	notify=notify_dir+vol_id
        infi_vol_id=int(vol_id[-5:])
        #print "*** VOL ID IS {}".format(vol_id)
        url="http://{}/api/rest/volumes/{}?approved=yes".format(ibox, infi_vol_id)
        #print "URL IS {}".format(url)
        try:
            outp = requests.delete(url=url,auth=creds)
        except Exception as E:
            print E
            abort(500)
        vol_data=get_vol_data(outp.json(),vol_id)
        ret_data={}
        #ret_data['volume_id']=vol_data['volumes']['id']
        ret_data['volume_id']=vol_id
        ret_data['create_at']=vol_data['volumes']['create_at']
        ret_data['status']='deleted'
        ret_data['result']='success'
        ret_data['snapshot_id']=""
        ret_data['notify_type']='volume_delete'
        try:
        	print "notify is {}".format(notify)
	    	notify_f=open(notify,'w')
	    	notify_f.writelines(json.dumps(ret_data))
	    	notify_f.close()
	    	notify_rm(notify)
	except Exception:
	    	pass
        #print str
        #return "kuku",200
        time.sleep(5)
        return ret_data, int(outp.status_code)

api.add_resource(VolumesList, "/api/v1/volumes")
api.add_resource(Volume, "/api/v1/volumes/<string:vol_id>")
app.run(debug=True, port=8080, host='0.0.0.0')
    