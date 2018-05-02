#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
该脚本适合已知一个根节点，在该根节点下添加组织结构。
结构内容是部门的名字

"""


import os
import json
import time
import uuid
import sys
import pprint
import pandas
import MySQLdb
import datetime as dt
from datetime import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

from config_file import  MYSQL_CONF_PATH, DB_STATUS
os.environ['HILLINSIGHT_MYSQL_CONF'] = MYSQL_CONF_PATH
os.environ['SKY_SERVER_MYSQL_ENV'] = DB_STATUS
from db_conn import _mysql_config
from common import add_hierarchy, generate_h_index_code_by_dpt_pid_for, update_child_index_code
from common import has_user_in_hierarchy

ROOT_INFO = {
    "hid": 333,
    "root_id":333,
    "dpt_name":"百丽"
}

SYNC_TYPE = {
    'add':1,
    'update':2,
    'delete':3,
    'none':4
}


#获取

db_slave = _mysql_config['sso']['slave']
db_master = _mysql_config['sso']['master']
def get_hinfo_by_hid(hid):

    sql = '''
        select * from ss_hierarchy where id=%d
    ''' % hid
    result = db_slave.query(sql)
    if not result:
        return {}
    hinfo = {}
    for one in result:
        hinfo["hid"] = one.get("id")
        hinfo['parent_id'] = one.get("parent_id") if one.get("parent_id") else 0
        hinfo['name'] = str(one.get("name"))
        hinfo['index_code'] = str(one.get("index_code"))
        hinfo['status'] = one.get('status')
        hinfo['display'] = one.get('display')
    return hinfo

def get_hid_by_unitId(unit_id):
    sql = '''
        select * from belle_unitcode_map_hierarchyid where unit_id=%d
    ''' % unit_id
    result = db_slave.query(sql)
    if not result:
        return 0
    return result[0].get('hierarchy_id')

def get_unitId_by_hid(hid):
    sql = '''
        select * from belle_unitcode_map_hierarchyid where hierarchy_id=%d
    ''' % hid
    result = db_slave.query(sql)
    if not result:
        return 0
    return result[0].get('unit_id')


def get_uinfo_to_sync(limit=0):
    sql = '''
        select id, unitId, fullName, unitCode, parentId, orgStatus, delFlag from belle_ehr_unit where is_sync=0 order by id
    '''
    if limit:
        sql += ' limit %d' % limit
    result = db_slave.query(sql)
    return list(result)

def add_unit_to_db(unitId, unit_name, u_parent_id, unit_code=''):
    p_hid = get_hid_by_unitId(u_parent_id)
    if u_parent_id:
        p_hinfo = get_hinfo_by_hid(p_hid)
        index_code = generate_h_index_code_by_dpt_pid_for(p_hid)
        hid = db_master.insert('ss_hierarchy', name=unit_name, parent_id=p_hid, type='部门',
                               index_code = index_code, display=p_hinfo.get('display', 0),
                               cuid=316, opuid=316, root_id=ROOT_INFO.get("root_id"))
        result = db_master.insert('belle_unitcode_map_hierarchyid', hierarchy_id=hid, unit_code=unit_code, unit_id=unitId)
        if result:
            update_sync_status(unitId, SYNC_TYPE.get('add', 1))
            return True
    return False


def check_change(unit_info, hinfo):
    sso_phid = hinfo.get("parent_id")
    belle_puid = unit_info.get("parentId")
    phid = get_hid_by_unitId(belle_puid)
    if sso_phid == phid and hinfo.get("name") ==unit_info.get("fullName") and hinfo.get("status") == unit_info.get("orgStatus"):
        result = update_sync_status(unit_info.get("unitId"), SYNC_TYPE.get('none'))
        return result
    return False

def update_unit_to_db(unit_info, hinfo):
    # 部门禁用，则直接跳出
    if unit_info.get("orgStatus") == 0:
        return False

    sso_phid = hinfo.get("parent_id")
    belle_puid = unit_info.get("parentId")
    phid = get_hid_by_unitId(belle_puid)
    #父节点还没有在ss_hierarchy中，暂时不做处理
    if not phid:
        return False
    update_str = ''
    index_code = ''
    p_hinfo = get_hinfo_by_hid(phid)
    display = p_hinfo.get('display', 0)

    if sso_phid != phid:

        update_str += 'parent_id=%d' % phid
        index_code = generate_h_index_code_by_dpt_pid_for(phid)
        update_str += ", index_code='%s'  " % index_code
        update_str += ', display=%d ' % display
    if unit_info.get('fullName') != hinfo.get('name'):
        update_str += ',' if update_str else ''
        update_str += " name='%s' " % MySQLdb.escape_string(unit_info.get('fullName'))
        pass
    if unit_info.get("orgStatus") != hinfo.get('status'):
        update_str += ',' if update_str else ''
        update_str += ' status=%d' % unit_info.get("orgStatus")
    if update_str:
        update_hierarchy_sql = 'update ss_hierarchy set ' + update_str + ' where id=%d ; ' % hinfo.get('hid')
        print update_hierarchy_sql
        result = db_master.query(update_hierarchy_sql)
        if result and sso_phid != phid:
            update_child_index_code(index_code, hinfo.get('index_code'), table_name='ss_hierarchy', display=display)
        update_sync_status(unit_info.get('unitId'), SYNC_TYPE.get('update'))
    return 0

def delete_unit_to_db(unit_info, hinfo):
    if unit_info.get("orgStatus") != 0:
        return False
    if not has_user_in_hierarchy(hinfo.get("hid")):
        return False

    if unit_info.get("orgStatus") != hinfo.get('status'):
        sql = '''
            update ss_hierarchy set status=0 where id=%d
        ''' % hinfo.get('hid')
        print sql
        result = db_master.query(sql)
        if result:
            update_sync_status(unit_info.get('unitId'), SYNC_TYPE.get('delete'))



def check_uinfo_and_hinfo(num=None):
    unifo_list = get_uinfo_to_sync(limit=num)
    for one in unifo_list:
        unit_id = one.get("unitId")
        puid = one.get('parentId')
        hid = get_hid_by_unitId(unit_id)
        unit_name = one.get('fullName')
        if not hid :
            #add_unit_to_db(unit_id, unit_name, puid)
            continue
        hinfo = get_hinfo_by_hid(hid)
        check_change(one, hinfo)
        # update_unit_to_db(one, hinfo)



def check_add(num=None):
    unifo_list = get_uinfo_to_sync(limit=num)
    n=1
    for one in unifo_list:
        unit_id = one.get("unitId")
        puid = one.get('parentId')
        hid = get_hid_by_unitId(unit_id)
        unit_name = one.get('fullName')
        if not hid :
            print n
            add_unit_to_db(unit_id, unit_name, puid)
            n += 1

def check_update(num=None):
    unifo_list = get_uinfo_to_sync(limit=num)
    for one in unifo_list:
        unit_id = one.get("unitId")
        hid = get_hid_by_unitId(unit_id)
        if hid and hid !=333 :
            hinfo = get_hinfo_by_hid(hid)
            update_unit_to_db(one, hinfo)


def check_delete(num=None):
    unifo_list = get_uinfo_to_sync(limit=num)
    for one in unifo_list:
        unit_id = one.get("unitId")
        hid = get_hid_by_unitId(unit_id)
        if hid and hid != 333:
            hinfo = get_hinfo_by_hid(hid)
            delete_unit_to_db(one, hinfo)


def update_sync_status(unitId, sync_type):
    sql = '''
        update belle_ehr_unit set is_sync=1, sync_type=%d where unitId=%d
    ''' % (sync_type, unitId)
    print "--------------update sync status--------"
    print sql
    result = db_master.query(sql)
    if result:
        return True
    return False

if __name__ == '__main__':
    #check_uinfo_and_hinfo()
    #check_add()
    #check_update()
    check_delete()




















