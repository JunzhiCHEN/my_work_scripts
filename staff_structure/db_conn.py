import sys
from hillinsight.storage import dbs
import os
import pprint
from config_file import MYSQL_CONF_PATH, DB_STATUS

# print MYSQL_CONF_PATH
# print DB_STATUS
os.environ['HILLINSIGHT_MYSQL_CONF'] = MYSQL_CONF_PATH
os.environ['SKY_SERVER_MYSQL_ENV'] = DB_STATUS
_mysql_config = {}
_mysql_env = ['online', 'offline']

def load_mysql_config():
    mysql_conf = os.getenv('HILLINSIGHT_MYSQL_CONF')
    mysql_env  = os.getenv('SKY_SERVER_MYSQL_ENV')
    conf_filename = MYSQL_CONF_PATH

    if mysql_conf != None and os.path.exists(mysql_conf.strip()):
        conf_filename = mysql_conf

    if mysql_env not in _mysql_env:
        #mysql_env = 'offline'
        mysql_env = 'offline'
    #print conf_filename, mysql_env
    dbconns = {}
    configs = []
    for line in open(conf_filename):
        line = line.strip()
        if line.startswith("#") or line == "":
            continue
        fields = line.split(",")
        config = {}
        for field in fields:
            (k,v) = (f.strip() for f in field.split("="))
            if k not in ("db","user","pw","host","port","master","online"):
                continue # ignore invalid key/value pair
            config[k] = v
        configs.append(config)
    for c in configs:
        master_or_slave = ("master" if c["master"] == "1" else "slave")
        m_or_s_bool = (True if c["master"] == "1" else False)
        on_or_offline = ("online" if c["online"] == "1" else "offline")
        on_or_off_bool = (True if c["online"] == "1" else False)

        if on_or_offline == mysql_env:
            db = dbs.create_engine(c['db'], master=m_or_s_bool, online=on_or_off_bool)

            if c['db'] not in dbconns:
                dbconns[c['db']] = {'master':None,'slave':None}
            dbconns[c['db']][master_or_slave] = db
    # print configs
    # print dbconns
    return dbconns

_mysql_config = load_mysql_config()

if __name__ == '__main__':
    pprint.pprint(_mysql_config)

