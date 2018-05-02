#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import json
import time
import uuid
import sys
import datetime as dt
from datetime import datetime
reload(sys)
sys.setdefaultencoding('utf-8')

from config_file import  MYSQL_CONF_PATH, DB_STATUS
os.environ['HILLINSIGHT_MYSQL_CONF'] = MYSQL_CONF_PATH
os.environ['SKY_SERVER_MYSQL_ENV'] = DB_STATUS
from db_conn import _mysql_config



db_sso = _mysql_config["sso"]["master"]
def add_hierarchy(name, parent_id, index_code, type='部门', root_id=333, display=1):
    # sql = '''
    #         insert into ss_hierarchy (name, parent_id, index_code, type, root_id)
    #         VALUES ('%s', %d, '%s', '%s', %d)
    #     ''' % (name, parent_id, index_code, type, root_id)
    # print sql
    # db_sso = _mysql_config["sso"]["master"]
    # result = db_sso.query(sql)

    result = db_sso.insert("ss_hierarchy", name=name, parent_id=parent_id,
                           type=type, root_id=root_id, index_code=index_code, display=display)
    return result


#生成当前级的索引code
def generate_code_for_num (num, length=4):

    num_str = str(num)
    complement_len = length-len(str(num))
    complement_str = ''
    if complement_len>0:
        complement_str = complement_len * '0'
    return complement_str+num_str
def get_index_code(dpt_ids, table_name="ss_internal_hierarchy"):
    db_sso = _mysql_config["sso"]["master"]
    sql = '''
        select id, index_code from %s where id in (%s) and status=1
    ''' % (table_name, (',').join([str(dpt_id) for dpt_id in dpt_ids]))
    result = db_sso.query(sql)
    ihid_map_index_code = {}
    for one in result:
        ihid_map_index_code[one["id"]] = one["index_code"]
    return ihid_map_index_code

def generate_h_index_code_by_dpt_pid_for(hid):
    db_sso = _mysql_config["sso"]["master"]
    sql = "select index_code from ss_hierarchy"
    if hid not in [0, None, "", "none"]:
        sql += " where parent_id=%d " % hid
    else:
        sql += ' where parent_id is null or parent_id=0'
    sql += " and status=1 order by index_code desc limit 1 "
    result = db_sso.query(sql)
    current_code = result[0]["index_code"] if result else ''
    child_code = current_code[-4:] if current_code else '0'
    next_code = current_code[0:-4] + generate_code_for_num(int(child_code)+1, 4)
    parent_index_code = get_index_code([hid], table_name="ss_hierarchy").get(hid, "")
    if not result and parent_index_code:
        next_code = parent_index_code + next_code
    return next_code

#根据表现层级父亲hid生成索引index_code
def generate_ih_index_code_by_dpt_pid_for(dpt_id):

    sql = "select index_code from ss_internal_hierarchy"
    if dpt_id not in [0, None, "", "none"]:
        sql += " where dpt_parent_id=%d " % dpt_id
    else:
        sql += ' where dpt_parent_id is null or dpt_parent_id=0'
    sql += " and status=1 order by index_code desc limit 1 "
    print sql
    result = db_sso.query(sql)
    current_code = result[0]["index_code"] if result else ""
    child_code = current_code[-4:] if current_code else '0'
    next_code = current_code[0:-4] + generate_code_for_num(int(child_code)+1, 4)
    parent_index_code = get_index_code([dpt_id], table_name="ss_internal_hierarchy").get(dpt_id, "")
    if not result and parent_index_code:
        next_code = parent_index_code + next_code
    return next_code


def update_child_index_code(new_p_index_code, old_parent_code, table_name, display=0):
    ihid_map_index_code = get_all_children_ihid_map_index_code(old_parent_code, table_name=table_name)
    if not ihid_map_index_code:
        return False
    for one in ihid_map_index_code:
        ihid_map_index_code[one] = new_p_index_code + ihid_map_index_code.get(one)[len(old_parent_code):]
    update_to_db(ihid_map_index_code, table_name, display)


def update_to_db(hid_map_code, table_name="ss_internal_hierarchy", display=0):
    if not hid_map_code:
        print "hid_map_code is {} "
        return False
    sql = "update %s set display= %d, index_code= case id " % (table_name, display)
    for hid, code in hid_map_code.items():
        sql += '''
             when %d then '%s'
        ''' % (hid, code)
    sql += " End where id in (%s); " % (',').join([str(one) for one in hid_map_code.keys()])
    print "----- update index_code for child dpt of %s -----" % table_name
    print sql
    result = db_sso.query(sql)
    print result
    return result

def get_all_children_ihid_map_index_code(ih_parent_index_code, table_name="ss_internal_hierarchy"):

    sql = "select id, index_code from %s where index_code like '%s%%' and length(index_code) > length('%s') and status=1"  % (table_name, ih_parent_index_code, ih_parent_index_code)
    result = db_sso.query(sql)
    ihid_map_index_code = {}
    for one in result:
        ihid_map_index_code[one["id"]] = one["index_code"]
    return ihid_map_index_code


def has_user_in_hierarchy(hid):
    sql = '''
        select * from ss_user_hierarchy_relation where status=1 and hierarchy_id=%d
    ''' % hid
    result = db_sso.query(sql)
    if result :
        return True
    return False

