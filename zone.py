import json
import os
from infinisdk import InfiniBox
global zones 
id_str="546c4f25-FFFF-FFFF-{}-{}"
box_serial_len=4
volume_id_len=12
import pprint
pp = pprint.PrettyPrinter(indent=4)


def get_zones_data (zone_file):
    with open('{}/{}'.format(scriptpath, zone_file)) as f:
        zones_data=json.load(f)
    return zones_data

def set_box_hexa(zones):
	for zone in zones['zones']:
		zone['serial_hexa']=hex(zone['serial_dec'])
	return zones
		

def get_box_by_par(**kwargs):
		zs=kwargs['zones']
		for zone in zs['zones']:
    			if zone[kwargs['par']] == kwargs['val']:
    					return zone[kwargs['req']]
		return None


def box_login(zones,action):
 	for zone in zones['zones']:
 		try:
 			
 			if action == 'login':
 				ibox=InfiniBox(zone['box_ip'],(zone['box_user'],zone['box_password']))
 				ibox.login()
 				zone['ibox']=ibox
 			elif action == 'logout':
 				zone['ibox'].logout()
 				zone['ibox']=None
 			else:
 				print("invalid action {}").format(action)
 		except Exception as E:
 			zone['ibox']=repr(E)

def box_auth(box_id):
    for zone in zones['zones']:
        if box_id == zone['box_ip']:
            return zone['box_user'], zone['box_password']

def fix_str(string,req_len):
    embedding_zeros=req_len-len(str(string))
    new_str=embedding_zeros*'0'+str(string)
    return new_str

def encode_vol_by_id(**kwargs):
	box=kwargs['val']
	vol=kwargs['id']
	req_type=kwargs['type']
	zones=kwargs['zones']
	box_hexa=get_box_by_par(par=req_type,req='serial_hexa',val=box,zones=zones)
	box_hexa=fix_str(box_hexa.replace('0x',''),box_serial_len)
	vol=fix_str(vol, volume_id_len)
	vol_id=id_str.format(box_hexa, vol)
	return vol_id

def decode_vol_by_id(vol,vtype,zones):
	#box='0x'+(vol.split('-')[3].replace('0',''))
	box=hex(int(vol.split('-')[3],16))
	vol_id=vol.split('-')[4]
	box_val=get_box_by_par(par='serial_hexa',req=vtype,val=box,zones=zones)
	return box_val, vol_id


scriptpath = os.path.dirname(os.path.abspath(__file__))
zones=get_zones_data('./zones.json')
set_box_hexa(zones)
