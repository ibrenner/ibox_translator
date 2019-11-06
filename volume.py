from flask import Flask, request,abort,jsonify,Response
from flask_restful import Api, Resource, reqparse
import requests
from requests.auth import HTTPBasicAuth
import urllib3
urllib3.disable_warnings()
import arrow
from infinisdk import InfiniBox
from capacity import GB,GiB
from zone import *
from shared import *
import time
import random, string
import json
import subprocess
from infi.dtypes.iqn import make_iscsi_name
from time import gmtime, strftime

### Wrapper 
def loggin_in_out(func):
    def wrapper(*args,**kwargs):
        box_login(zones,'login')
        return func(*args,**kwargs)
        #print "logging out"
        box_login(zones,'logout')
    return wrapper

def get_host(system,host_name):
    name=host_name.replace(':','%')
    host=system.hosts.find(name=name).to_list()
    if host:
        return host[0]
    else:
        address=make_iscsi_name(host_name)
        host=system.hosts.create(name=name)
        host.add_port(address)
        return host

def check_iqn_logged_in(system,iqn):
    initators=system.initiators.to_list()
    #print "initators: {}".format(initators)
    for init in initators:
        if init.get_address() == iqn:
            return False
    return True

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



## To be replaced with the actual values
loggedout_attempts=3
loggedout_interval=3
creds = HTTPBasicAuth('admin', '123456')

# Constants
onegig = 1000000000
service_id='d4a44b0a-e3c2-4fec-9a3c-1d2cb14328f9'
date_format='YYYY-MM-DD HH:mm:ss'
id_len=5
opts_pars = { 'volume_type': '' , 'iscsi_init': '', 'image_id': '', 'bootable': 0, 'zone_code': 0}
#iscsi_init=''
mandatory_pars = ['name','size']
vol_name_length=10


## Functions
ts=lambda now: strftime("%Y-%m-%d %H:%M:%S", gmtime())
new_size = lambda  size: size/1000/1000/1000 
def new_date(date):
	tsa=arrow.get(str(date)[:-3])
	return tsa.format(date_format)

def set_new_id(id):
	embedding_zeros=id_len-len(str(id))
	new_id=id_str+embedding_zeros*"0"+str(id)
	return new_id

	
def add_metadata(volume):
    ret_dict={}
    metadata=volume.get_all_metadata()
    for key in metadata.keys():
        ret_dict[key]=metadata[key]
    return ret_dict


def get_vol_data(volume):
    return_json={}
    return_json['volumes']={}
    return_json['volumes'].update(add_metadata(volume))
    return_json['volumes']['size'] = volume.get_size().bits/8/1000000000
    return_json['volumes']['create_at'] = volume.get_created_at().format('YYYY-MM-DD HH:mm:ss')
    return_json['volumes']['lun_id'] = volume.get_id()
    return_json['volumes']['service_id'] = service_id
    if volume.is_mapped():
        return_json['volumes']['attach_status'] = 'online'
    else:
        return_json['volumes']['attach_status'] = 'offline'
    if 'status' in volume.get_all_metadata():
        return_json['volumes']['status'] = volume.get_all_metadata()['status']
    else:
        return_json['volumes']['status'] = 'available'
    return return_json

class VolumesList(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('name', type=str, required=True, location='json')
        self.reqparse.add_argument('provtype', type=str, required=False, location='json', default='THIN')
        self.reqparse.add_argument('size', type=int, required=True, location='json')
        super(VolumesList, self).__init__()
   
    @loggin_in_out
    def get(self):
        outp=[]
        return_json={}
        if 'iscsi_init' in request.args:
            iscsi_filter=request.args['iscsi_init']
        else:
            iscsi_filter=False
        volumes=[]
        for box in zones['zones']:
            if ( isinstance(box['ibox'],InfiniBox)):
                volumes.extend(box['ibox'].volumes.find(type='master').to_list())
            
        for volume in volumes:
            if iscsi_filter and 'iscsi_init' in volume.get_all_metadata().keys() and volume.get_metadata_value('iscsi_init') != iscsi_filter:
                continue 
            else: 
                cur_vol=get_vol_data(volume)
                outp.append(cur_vol['volumes'])
        return_json['volumes']=outp
        return return_json, 200
    
    @loggin_in_out
    def post(self):
        body=request.json
        for mandatory_key in mandatory_pars:
            if mandatory_key not in body['volumes']: ##1
                print("mandatory_key {} can't be found").format(mandatory_key)
                raise InvalidUsage('Mandatory key cannot be found', status_code=410)
        system=get_box_by_par(par="name",req="ibox",val=body['volumes']['zone_code'],zones=zones)
        pool=system.pools.to_list()[0]
        if not system:
            return Response(status = 404)
        new_name=generate_random_name(vol_name_length)
        volume=system.volumes.create(pool=pool,size=body['volumes']['size']*GB,name=new_name)
        if body['volumes']['iscsi_init']:
            host=get_host(system, body['volumes']['iscsi_init'])
            host.map_volume(volume)
            volume.set_metadata('status', 'in-use')	
        volume.set_metadata('name',body['volumes']['name'])
        volume.set_metadata('iscsi_init',body['volumes']['iscsi_init'])
        volume.set_metadata('status','available')
        new_id=encode_vol_by_id(val=system,id=volume.get_id(),type='ibox',zones=zones)
        volume.set_metadata('id',new_id)
        for optional_key in opts_pars:
            if optional_key in body['volumes']:
                volume.set_metadata(optional_key,body['volumes'][optional_key])
            else:
                volume.set_metadata(optional_key,opts_pars[optional_key])
        # url="http://{}/api/rest/volumes/{}".format(system.get_name(),volume.get_id())
        # vol_infi_data=requests.get(url=url,auth=creds)
        global iscsi_init
        # global volume_type
        if 'iscsi_init' in body['volumes'].keys():
            iscsi_init=body['volumes']['iscsi_init']
        else:
            iscsi_init=''
        vol_data=get_vol_data(volume)
        notify_vol={}
        notify_vol={'volume_id':new_id, 'id':"", 'status':'available', 'notify_type':'volume_create'}
        try:
            thread_a = NotifyRM(notify_vol)
            thread_a.start()            
        except Exception as E:
            str="Failed! {} ; notify is {}".format(E,notify)
            print(str) 		
            #return str,400
        vol_data['volumes']['status']='creating'
        return vol_data, 200

class Volume(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
    

    @loggin_in_out
    def get(self, vol_id):
        system,vol=decode_vol_by_id(vol_id,'ibox',zones)
        try:
            volume=system.volumes.find(id=vol)[0]
        except Exception: #outp_json['error'] or not outp_json['result']:
            return Response(status = 404)
        return_json=get_vol_data(volume)
        return return_json, 200
        
    def post(self, id):
       pass

    def put(self, id):
        body=request.json
        print(body.keys())
        string="id is {} data is {}".format(id, body)
        return string,400
    
    @loggin_in_out
    def delete(self, vol_id):
        system,vol=decode_vol_by_id(vol_id,'ibox',zones)
        try:
            volume=system.volumes.find(id=int(vol))
            if volume:
                if volume[0].is_mapped():
                    volume[0].unmap()
                    volume[0].delete()
                #vol_data=get_vol_data(volume[0])
#                volume[0].delete()
            else: 
                return Response(status = 200)
        except Exception as E:
            print(E)
            return E.message, 404
        ret_data={'volume_id':vol_id, 'id':"", 'status':'deleted', 'notify_type':'volume_delete'}
        try:
            thread_b = NotifyRM(ret_data)
            thread_b.start()	    	
        except Exception:
            pass
        return Response(status = 200)

class VolumesAttachment(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()

    @loggin_in_out    
    def post(self):
        body=request.json
        status='success'
        for volume in body['volume']['volumes']:
            system,vol_inf_id=decode_vol_by_id(volume['volume_id'],'ibox',zones)
            host=get_host(system,body['volume']['iscsi_init'])
            vol=system.volumes.find(id=vol_inf_id).to_list()
            if not vol:
                pass
            if body['volume']['action'].upper() == "ATTACH":
                try: 			
                    host.map_volume(vol[0])
                except Exception as E:
                    print("Execption {}").format(E)
                    status='fail'
            elif body['volume']['action'].upper() == "DETACH":
                ## TASK - Add tests here to find if volume is 'in use'
                for val in range(loggedout_attempts):
                # for attempt in xrange(loggedout_attempts):
                    val=check_iqn_logged_in(system,body['volume']['iscsi_init'])
                    if val:
                        host.unmap_volume(vol[0])
                        vol[0].set_metadata('status', 'available')
                        status='success'
                        break
                    else:
                        time.sleep(loggedout_interval)
                        print("Host is still online")
                        status='fail'
            else:
                status='fail'

        body['status']=status
        return body, 200 ## change ret codes

class VolumeExpand(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
    
    @loggin_in_out        
    def post(self,vol_id):
        body=request.json
        #volume=vol_id
        new_size=int(body['volume']['size'])
        system,vol_inf_id=decode_vol_by_id(vol_id,'ibox',zones)
        volume_object=system.volumes.find(id=vol_inf_id).to_list()
        if not volume_object:
            return 'Volume Not Found', 404
        volume_size=volume_object[0].get_size().bits/8/1000000000
        if volume_size > new_size:
            return 'Volume is already bigger', 405
        cap_to_resize=(new_size-volume_size)*GB
        try:
            volume_object[0].resize(cap_to_resize)
        except Exception as E:
            print("Caught Exception {}").format(E)
            return 'Exception', 500
        ret_data={'volume_id':vol_id, 'id':"", 'status':'available', 'notify_type':'volume_extend'}
        try:
            thread_a = NotifyRM(ret_data)
            thread_a.start()  
        except Exception:
            pass
        return Response(status = 200)
    
    
