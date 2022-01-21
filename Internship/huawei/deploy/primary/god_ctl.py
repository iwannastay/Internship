import sys
import os
import time
import re
import logging
import subprocess
import json
from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import yaml
from kubernetes import client
from kubernetes import config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream

COMMAND_INDEX = 1
ARG_START_INDEX = 2
USER = 'dbuser'
MAX_FILE_SIZES = 5 * 1024 * 1024  # 限定文件最大为5M
READ_FILE_SIZES_MB = 1 * 1024 * 1024  # 限定每次读取1M文件
SUCCESS = 0
FAILED = 1

SINGLE = 0
PRIMARY = 1

NAMESPACE = 'manager'
GAUSS_ROOT = "/opt/data1/greenopengauss"
GAUSS_HOME = GAUSS_ROOT + "/app"
GS_CTl_PATH = GAUSS_HOME + "/bin/gs_ctl"
GS_INITDB_PATH = GAUSS_HOME + "/bin/gs_initdb"
GS_DATA_PATH = GAUSS_ROOT + "/data"
KUBE_CONFIG_PATH = "config"

LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(module)s - %(lineno)d : %(funcName)s] - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename=None,
                    filemode='a',
                    format=LOG_FORMAT,
                    datefmt=DATE_FORMAT,
                    level=logging.INFO)
logger = logging.getLogger(__name__)
usage_info = '''
Usage:
python3 opengauss_ctl.py create-db-instance -file <filename>
python3 opengauss_ctl.py delete-db-instance -instance <instance>
python3 opengauss_ctl.py add-database -instance <instance> -dbnames <db name list>
python3 opengauss_ctl.py remove-database -instance <instance> -dbnames <db name list>
python3 opengauss_ctl.py start-db-instance -instance <instance>
python3 opengauss_ctl.py stop-db-instance -instance <instance>
Example:
python3 opengauss_ctl.py create-db-instance -file zenith_template.json
python3 opengauss_ctl.py delete-db-instance -instance xiangyu
python3 opengauss_ctl.py add-database -instance xiangyu -dbnames dbname01,dbname02
python3 opengauss_ctl.py remove-database -instance xiangyu -dbnames dbname01,dbname02
python3 opengauss_ctl.py start-db-instance -instance xiangyu
python3 opengauss_ctl.py stop-db-instance -instance xiangyu
'''


# # no use
# class Member:
#     def __init__(self):
#         self.id = None
#         self.type = None
#         self.host = None
#         self.port = None
#         self.instance_dir = None


class Instance:
    def __init__(self):
        config.load_kube_config(KUBE_CONFIG_PATH)
        self.core_v1 = client.CoreV1Api()
        self.app_v1 = client.AppsV1Api()
        self.gauss_root = GAUSS_ROOT
        self.gauss_home = GAUSS_HOME
        self.gs_ctl_path = GS_CTl_PATH
        self.mandatory = []
        self.optional = []
        self.members = []  #
        self.name = None
        self.mode = None


class CreateDBInstance(Instance):
    def __init__(self):
        super().__init__()
        self.gs_initdb_path = GS_INITDB_PATH
        self.password = "Changeme_123"
        self.default_port = 5432
        self.mandatory = ["file"]
        self.template = ["id", "host", "port"]
        self.optional = ["class"]

    def check_template(self, file_dict):
        for item in ["name", "members"]:
            if item not in file_dict:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return FAILED

        self.name = file_dict.get("name")
        members = file_dict.get("members")
        if not members:
            logger.error("members can't be empty")
            return FAILED

        for member in members:
            for item in self.template:
                if item not in member:
                    logger.error("[ERROR]argument(%s) must exist" % item)
                    return FAILED
            member["dir"] = os.path.join(GS_DATA_PATH, member.get("id"))

        # TODO: check element
        self.members = members
        return SUCCESS

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return FAILED

        file_dict = get_file(args_map.get("file"))
        if not file_dict:
            return FAILED

        if self.check_template(file_dict) != SUCCESS:
            return FAILED

        if len(self.members) == 1:
            self.mode = SINGLE
        else:
            self.mode = PRIMARY

        return SUCCESS

    def exec(self, args_map):
        if self.check_param(args_map) != SUCCESS:
            return FAILED
        if self.create_db_instance() != SUCCESS:
            self.clear_instance()
            return FAILED
        return SUCCESS

    def clear_instance(self):
        return
        # instance = self.instance
        # delete_sts(self.app_v1, instance)
        # rmdb_cmd = "rm -rf /home/dbuser/gaussdata/%s" % instance  # self.instance_dir
        # logger.info(rmdb_cmd)
        # result, status = exec_cmd(rmdb_cmd)
        # if status != 0:
        #     logger.error("[ERROR]remove instance(%s) dir failed" % instance)
        #     return FAILED
        # logger.info("[INFO]delete instance(%s) success" % instance)

    def create_db_instance(self):
        instance = self.name
        if is_sts_exist(self.app_v1, instance, NAMESPACE):
            logger.error("[INFO]instance(%s) is running" % instance)
            return FAILED

        logger.info("[INFO]Launching sts (%s)" % instance)
        _, ret = create_sts(self.app_v1, instance, len(self.members))
        if not ret:
            logger.info("[INFO]Launching sts (%s) success" % instance)
        else:
            logger.error("[INFO]Launching sts (%s) failed" % instance)

        time_out = 1
        while time_out != 10 and not is_sts_ready(self.app_v1, instance, NAMESPACE):
            logger.info("Waiting sts %s ready" % instance)
            time.sleep(time_out)
            time_out += 1

        if time_out >= 10:
            logger.error("[INFO]Launching sts (%s) failed" % instance)
            return FAILED
        logger.info("Sts %s RUNNING" % instance)

        logger.info("Start initialize instance %s" % instance)
        if SUCCESS != self.init_instance():
            logger.error("[INFO]Init instance (%s) failed" % instance)
            return FAILED
        logger.info("Start instance %s" % instance)
        for member in self.members:
            if self.start_db(member) != SUCCESS:
                logger.error("[INFO]Start db_instance (%s) failed" % member.get("id"))
                return FAILED
        return SUCCESS

    def start_db(self, member):
        instance_id = member.get("id")
        instance_dir = member.get("dir")
        logger.info("[INFO]start instance")
        if self.mode == SINGLE:
            startdb_cmd = "%s start -D %s -Z single_node" % (self.gs_ctl_path, instance_dir)
        elif instance_id.endswith("-0"):
            startdb_cmd = "%s start -D %s -M primary" % (self.gs_ctl_path, instance_dir)
        else:
            startdb_cmd = "%s build -D %s -b full" % (self.gs_ctl_path, instance_dir)

        logger.info(startdb_cmd)
        result, status = exec_remote_cmd(startdb_cmd, self.core_v1, instance_id, NAMESPACE)
        if re.search("server started", "\n".join(result)) is not None or \
                re.search("another server", "\n".join(result)) is not None:
            logger.info("[INFO]create instance(%s) success: %s" % (instance_id, result))
        else:
            logger.error("[ERROR]create instance(%s) failed" % instance_id, result)
            return FAILED
        return SUCCESS

    def init_db(self, member):
        instance_id = member.get("id")
        instance_dir = member.get("dir")
        logger.info("[INFO]init instance")
        initdb_cmd = "%s -w %s -D %s --nodename '%s' --locale='en_US.UTF-8' -U %s" % (
            self.gs_initdb_path, self.password, instance_dir, "sg_node", USER)
        result, status = exec_remote_cmd(initdb_cmd, self.core_v1, instance_id, NAMESPACE)
        result_txt = "\n".join(result)
        if re.search("error|Error", result_txt) is None and re.search("Success", result_txt) is not None:
            logger.info("[INFO]init instance(%s) success" % instance_id)
        else:
            logger.error("[ERROR]init instance(%s) failed: %s" % (instance_id, result))
            return FAILED

        config_cmd = "echo \"host all all 0.0.0.0/0 sha256\" >> {2} \
                    && sed -i \"/^#listen_addresses =/c\\listen_addresses = '*'\" {1} \
                    && sed -i '/^#port =/c\\port = {0}' {1}".format(
            member.get("port"),
            os.path.join(instance_dir + "/postgresql.conf"),
            os.path.join(instance_dir + "/pg_hba.conf"))
        _, ret = exec_remote_cmd(config_cmd, self.core_v1, instance_id, NAMESPACE)
        if ret:
            logger.error("[ERROR]config instance(%s) failed: %s" % (instance_id, result))
            return FAILED
        return SUCCESS

    def config_replconninfo(self):
        # TODO: connect with everyone?
        for index, member in enumerate(self.members):
            instance_id = member.get("id")
            local_host = member.get("host")
            local_port = int(member.get("port"))
            for _index, other in enumerate(self.members):
                if other.get("id") == instance_id:
                    continue
                name = "replconninfo" + str(_index + 1)
                other_port = int(other.get("port"))
                replconninfo_cmd = "{0} = '" \
                                   "localhost={1} localport={2} localheartbeatport={3} localservice={4} " \
                                   "remotehost={5} remoteport={6} remoteheartbeatport={7} remoteservice={8}'" \
                    .format(name,
                            local_host, local_port + 1, local_port + 5, local_port + 4,
                            other.get("host"), other_port + 1, other_port + 5, other_port + 4)
                result, ret = exec_remote_cmd(
                    '''echo "%s" >> %s''' % (replconninfo_cmd, os.path.join(member.get("dir"), "postgresql.conf")),
                    self.core_v1, instance_id, NAMESPACE)

                if ret:
                    logger.error("[ERROR]config instance(%s) failed: %s" % (instance_id, result))
                    return FAILED
        return SUCCESS

    def init_instance(self):
        for member in self.members:
            if self.init_db(member) != SUCCESS:
                logger.error("[INFO]Init instance (%s) failed" % member.get("id"))
                return FAILED
        if self.mode == PRIMARY:
            if self.config_replconninfo() != SUCCESS:
                logger.error("[INFO]Config replconninfo failed")
                return FAILED
            # TODO not here but post start
        return self.create_remote_user()

    def create_remote_user(self):
        logger.info("[INFO]Create nobody for %s" % self.name)
        return SUCCESS


class DeleteDBInstance(Instance):
    def __init__(self):
        super().__init__()
        self.mandatory = ["instance"]
        self.instance = None
        self.instance_dir = None

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return FAILED
        self.instance = args_map.get("instance")
        self.instance_dir = GS_DATA_PATH + "/" + self.instance
        return SUCCESS

    def exec(self, args_map):
        if SUCCESS != self.check_param(args_map):
            return FAILED
        instance = args_map.get("instance")
        _, status = delete_sts(self.app_v1, instance)
        if status != SUCCESS:
            logger.error("[ERROR]delete instance(%s) failed" % instance)
            return FAILED
        else:
            rmdb_cmd = "rm -rf /home/dbuser/gaussdata/%s-*" % instance  # self.instance_dir
            _, status = exec_cmd(rmdb_cmd)
            if status != SUCCESS:
                logger.error("[ERROR]remove instance(%s) dir failed" % instance)
                return FAILED
            logger.info("[INFO]delete instance(%s) success" % instance)
            return SUCCESS


class AddDatabase(Instance):
    def __init__(self):
        super().__init__()
        self.mandatory = ["instance", "dbnames"]
        self.dbname_list = None
        self.password = None
        self.port = None
        self.gauss_helper = None

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return FAILED
        instance = args_map.get("instance")
        instance_id = get_id(instance)
        instance_dir = GS_DATA_PATH + "/" + instance_id

        if not is_sts_exist(self.app_v1, instance, NAMESPACE):
            logger.error("[ERROR]Sts (%s) must exist" % instance)
            return FAILED
        result, status = exec_remote_cmd(
            "ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep | awk '{print $2}'" % instance
            , self.core_v1, instance_id, NAMESPACE)
        if len(result) == 0:
            logger.error("[ERROR]instance(%s) has not been created or started" % instance_id)
            return FAILED
        result, status = exec_remote_cmd(
            "sed -n '/^port/p' %s/postgresql.conf | awk '{print $3}'" % instance_dir,
            self.core_v1, instance_id, NAMESPACE)
        self.port = result[-1]
        db_names = args_map.get("dbnames")
        if not db_names:
            logger.error("dbnames(%s) can't be empty" % db_names)
            return FAILED
        str_list = db_names.split(',')
        for item in str_list:
            if not str(item):
                logger.error("[ERROR]dbnames(%s) is invalid!" % db_names)
                return instance
        self.dbname_list = str_list
        self.password = "Changeme_123"

        return SUCCESS

    def create_access_db(self):
        ret, result = self.gauss_helper.exec_sql("SELECT datname FROM pg_database WHERE datname = 'ossdb'", fetch=True)
        if result:
            logger.info("database ossdb already exists")
            return SUCCESS
        create_db_cmd = "CREATE DATABASE ossdb WITH ENCODING = 'UTF8' template template0 lc_collate = 'C' LC_CTYPE= 'C' DBCOMPATIBILITY 'PG' CONNECTION LIMIT = 1024"
        ret, result = self.gauss_helper.exec_sql(create_db_cmd)
        if not ret:
            logger.error("create accessdb(ossdb) failed: %s" % result)
            return FAILED
        logger.info("create accessdb(ossdb) success")
        return SUCCESS

    def exec(self, args_map):
        if SUCCESS != self.check_param(args_map):
            return FAILED
        self.gauss_helper = GaussHelper(self.password, self.port)
        if self.create_access_db() != SUCCESS:
            return FAILED
        dbname_list = args_map.get("dbnames").split(',')
        ret, result = self.gauss_helper.exec_sql("SELECT datname FROM pg_database", fetch=True)
        if not result:
            logger.error("Failed to query the database list")
            return FAILED
        exist_dbnames = [item[0] for item in result]
        duplicate_dbnames = [item for item in dbname_list if item in exist_dbnames]
        if duplicate_dbnames:
            logger.error("[ERROR]The following database(%s) already exists" % duplicate_dbnames)
            return FAILED
        for dbname in dbname_list:
            sql_add_database = [
                "REVOKE ALL ON SCHEMA PUBLIC FROM PUBLIC",
                "CREATE TABLESPACE {0} RELATIVE LOCATION 'tablespace/{0}' MAXSIZE '102400M'".format(dbname),
                "CREATE USER {0} ENCRYPTED Password '{1}' NOSYSADMIN NOINHERIT CONNECTION LIMIT 500 PERM SPACE '102400M' TEMP SPACE '102400M' SPILL SPACE '102400M';".format(
                    dbname, self.password),
                "ALTER TABLESPACE {0} OWNER TO {0}".format(dbname),
                "ALTER USER {0} SET search_path TO {0}".format(dbname),
                "ALTER USER {0} SET default_tablespace TO {0}".format(dbname),
                "CREATE TABLESPACE {0}_tempdb RELATIVE LOCATION 'tablespace/{0}_tempdb' MAXSIZE '102400M'".format(
                    dbname),
                "ALTER USER {0} SET temp_tablespaces TO {0}_tempdb".format(dbname)
            ]
            for item in sql_add_database:
                ret, result = self.gauss_helper.exec_sql(item)
                if not ret:
                    logger.error("create database(%s) failed" % dbname)
                    return FAILED
        logger.info("create database(%s) success" % dbname_list)
        return SUCCESS


class RemoveDatabase(Instance):
    def __init__(self):
        super().__init__()
        self.mandatory = ["instance", "dbnames"]
        self.dbname_list = None
        self.password = None
        self.port = None
        self.gauss_helper = None

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return FAILED
        instance = args_map.get("instance")
        instance_id = get_id(instance)
        instance_dir = GS_DATA_PATH + '/' + instance_id

        result, status = exec_remote_cmd(
            "ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep | awk '{print $2}'" % instance
            , self.core_v1, instance_id, NAMESPACE)
        if len(result) == 0:
            logger.error("[ERROR]instance(%s) has not been created or started" % instance)
            return FAILED
        result, status = exec_remote_cmd(
            "sed -n '/^port/p' %s/postgresql.conf | awk '{print $3}'" % instance_dir,
            self.core_v1, instance_id, NAMESPACE)
        self.port = result[-1]
        dbnames = args_map.get("dbnames")
        if not dbnames:
            logger.error("dbnames(%s) can't be empty" % dbnames)
            return FAILED
        str_list = dbnames.split(',')
        for item in str_list:
            if not str(item):
                logger.error("dbnames(%s) can't be empty" % dbnames)
                return FAILED
        self.dbname_list = str_list
        self.password = "Changeme_123"
        return SUCCESS

    def exec(self, args_map):
        if SUCCESS != self.check_param(args_map):
            return FAILED
        self.gauss_helper = GaussHelper(self.password, self.port)
        dbname_list = args_map.get("dbnames").split(',')
        for dbname in dbname_list:
            sql_remove_database = [
                "DROP TABLESPACE {0}_tempdb".format(dbname),
                "ALTER TABLESPACE {0} OWNER TO {1}".format(dbname, USER),
                "DROP USER IF EXISTS {0} CASCADE".format(dbname),
                "DROP TABLESPACE IF EXISTS {0}".format(dbname)
            ]
            for item in sql_remove_database:
                ret, result = self.gauss_helper.exec_sql(item)
                if not ret:
                    logger.error("remove database(%s) failed" % dbname)
                    return FAILED
        logger.info("delete database(%s) success" % dbname_list)
        return SUCCESS


class StartDBInstance(Instance):
    def __init__(self):
        super().__init__()
        self.instance = None
        self.mandatory = ["instance"]
        self.instance_dir = None

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return FAILED
        self.instance = args_map.get("instance")
        return SUCCESS

    def exec(self, args_map):
        if SUCCESS != self.check_param(args_map):
            return FAILED
        logger.info("[INFO]start instance")
        if SUCCESS != self.start_db_instance():
            self.clear_instance()
            logger.error("[INFO]start instance failed")
            return FAILED
        return SUCCESS

    def start_db_instance(self):
        instance = self.instance
        if not is_sts_exist(self.app_v1, instance, NAMESPACE):
            logger.error("[INFO]Sts (%s) not exists" % instance)
            return FAILED

        sts = self.app_v1.read_namespaced_stateful_set(instance, NAMESPACE)
        replicas = int(sts.metadata.annotations.get('replicas'))
        if replicas == 1:
            self.mode = SINGLE
        else:
            self.mode = PRIMARY
        _, status = scale_sts(self.app_v1, instance, replicas)
        if status != 0:
            logger.error("[ERROR]Scale sts (%s) failed" % instance)
            return FAILED
        else:
            logger.info("[INFO]Scale instance(%s) success" % instance)

        time_out = 1
        while time_out != 10 and not is_sts_ready(self.app_v1, instance, NAMESPACE):
            logger.info("Waiting sts %s ready" % instance)
            time.sleep(time_out)
            time_out += 1

        if time_out >= 10:
            logger.error("[INFO]Launching sts (%s) failed" % instance)
            delete_sts(self.app_v1, instance)
            return FAILED

        logger.info("Sts %s RUNNING" % instance)

        for index in range(replicas):
            instance_id = instance + "-%s" % index
            result, status = exec_remote_cmd("ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep" % instance_id
                                             , self.core_v1, instance_id, NAMESPACE)
            if len(result) != 0:
                logger.info("[INFO]instance(%s) is running" % instance_id)
                return FAILED

            if SUCCESS != self.start_db(instance_id):
                logger.error("[INFO]start instance(%s) failed" % instance_id)
                return FAILED

        return SUCCESS

    def start_db(self, instance_id):
        instance_dir = os.path.join(GS_DATA_PATH + "/" + instance_id)
        logger.info("[INFO]start instance")
        if self.mode == SINGLE:
            startdb_cmd = "%s start -D %s -Z single_node" % (self.gs_ctl_path, instance_dir)
        elif instance_id.endswith("-0"):
            startdb_cmd = "%s start -D %s -M primary" % (self.gs_ctl_path, instance_dir)
        else:
            startdb_cmd = "%s build -D %s -b full" % (self.gs_ctl_path, instance_dir)

        result, status = exec_remote_cmd(startdb_cmd, self.core_v1, instance_id, NAMESPACE)
        if re.search("server started", "\n".join(result)) is not None or \
                re.search("another server", "\n".join(result)) is not None:
            logger.info("[INFO]Start instance(%s) success: %s" % (instance_id, result))
        else:
            logger.error("[ERROR]Start instance(%s) failed" % instance_id, result)
            return FAILED
        return SUCCESS

    def clear_instance(self):
        return
        # instance = self.instance
        # delete_sts(self.app_v1, instance)
        # rmdb_cmd = "rm -rf /home/dbuser/gaussdata/%s" % instance  # self.instance_dir
        # logger.info(rmdb_cmd)
        # result, status = exec_cmd(rmdb_cmd)
        # if status != 0:
        #     logger.error("[ERROR]remove instance(%s) dir failed" % instance)
        #     return FAILED
        # logger.info("[INFO]delete instance(%s) success" % instance)


class StopDBInstance(Instance):
    def __init__(self):
        super().__init__()
        self.instance = None
        self.mandatory = ["instance"]

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return FAILED
        instance = args_map.get("instance")
        self.instance = instance
        return SUCCESS

    def exec(self, args_map):
        if SUCCESS != self.check_param(args_map):
            return FAILED
        logger.info("[INFO]stop instance")

        if SUCCESS != self.stop_db_instance():
            logger.error("[INFO]start instance failed")
            return FAILED
        return SUCCESS

    def stop_db_instance(self):
        instance = self.instance

        if not is_sts_exist(self.app_v1, instance, NAMESPACE):
            logger.error("[INFO]Sts (%s) not exist" % instance)
            return FAILED

        replicas = self.app_v1.read_namespaced_stateful_set_scale(instance, NAMESPACE).spec.replicas
        for index in range(replicas - 1, -1, -1):
            instance_id = instance + "-%s" % index
            instance_dir = os.path.join(GS_DATA_PATH + "/" + instance_id)
            result, status = exec_remote_cmd("ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep" % instance_id
                                             , self.core_v1, instance_id, NAMESPACE)
            if len(result) == 0:
                logger.info("[INFO]instance(%s) is not running" % instance_id)
            else:
                stopdb_cmd = "%s stop -D %s" % (self.gs_ctl_path, instance_dir)
                logger.info(stopdb_cmd)
                result, status = exec_remote_cmd(stopdb_cmd, self.core_v1, instance_id, NAMESPACE)
                if re.search("server stopped", "\n".join(result)) is not None:
                    logger.info("[INFO]stop instance(%s) success" % instance_id)
                else:
                    logger.warning("[ERROR]stop instance(%s) failed: %s" % (instance_id, result))

            # scale sts to 0
            _, status = scale_sts(self.app_v1, instance, index)
            if status != 0:
                logger.error("[ERROR]Scale sts (%s) to %s failed" % (instance, index))
                return FAILED
            else:
                logger.info("[INFO]Scale instance(%s) to %s success" % (instance, index))
        return SUCCESS


def get_id(instance: str):
    # if instance.endswith(r'-[0-9]'):
    #     return instance
    return instance + '-0'


def exec_remote_cmd(command, api_instance, name, namespace):
    result = []
    remote_command = [
        'bash',
        '-c',
        'source /home/dbuser/.bashrc && ' +
        command
    ]

    if not is_pod_running(api_instance, name, namespace):
        logging.error("Pod %s not running in %s" % (name, namespace))
        return None, FAILED

    logger.info("the remote command to be executed is %s" % command)
    resp = stream(api_instance.connect_get_namespaced_pod_exec,
                  name,
                  namespace,
                  command=remote_command,
                  stderr=True, stdin=True,
                  stdout=True, tty=False
                  , _preload_content=False)
    while resp.is_open():
        resp.update(timeout=1)
        if resp.peek_stdout():
            result.append(resp.read_stdout())
        if resp.peek_stderr():
            logger.error("ERROR: %s" % resp.read_stderr())
    return result, resp.returncode


def exec_cmd(command):
    logger.info("the command to be executed is %s" % command)
    with subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT) as process:
        stream = process.stdout
        result_bytes = stream.readlines()
        result = []
        for line in result_bytes:
            result.append(line.decode(encoding='utf-8').strip('\n'))
        try:
            stream.close()
        except BaseException:
            logger.error("close stream exception")
        # wait must be after stream reading, otherwise cmd being executed maybe
        # blocked if it is print too many strings, which makes pipe full.
        try:
            process.wait(None)
        except BaseException:
            logging.error("process wait get exception")
            try:
                process.kill()
            except OSError as exception:
                logger.error("process kill get exception")
        status = process.returncode
    return result, status


def _get_args_map(command_args):
    args_map = {}
    args_index_max = len(command_args) - 1
    for index in range(len(command_args)):
        option = command_args[index]
        if not option.startswith('-'):
            continue
        option = command_args[index].replace("-", "")
        if index >= args_index_max:
            args_map[option] = None
            continue
        option_value = command_args[index + 1]
        if option_value.startswith('-'):
            args_map[option] = None
            continue
        args_map[option] = option_value

    return args_map


def print_screen(content, append_new_line=True):
    if append_new_line:
        content += "\n"
    sys.stdout.write(content)
    sys.stdout.flush()


def get_file(path):
    logger.info("get_file starting: %s" % path)
    if not os.path.isfile(path):
        logger.error("The file does not exist")
        return None

    if not re.match('.*\\.json$', path):
        logger.error("Incorrect file suffix type!")
        return None

    if os.stat(path).st_size > MAX_FILE_SIZES:
        logger.error("The file size cannot exceed %.2f MB!" % float(MAX_FILE_SIZES / (1024 * 1024)))
        return None

    try:
        with open(path, encoding="utf-8") as text:
            text_read = ''
            count = 0
            while True:
                segmentation_read = text.read(
                    int(READ_FILE_SIZES_MB))
                if count * READ_FILE_SIZES_MB > MAX_FILE_SIZES and segmentation_read:
                    logger.error("The actual file size exceed %.2f MB!" % float(MAX_FILE_SIZES / (1024 * 1024)))
                    return None
                if count * READ_FILE_SIZES_MB <= MAX_FILE_SIZES:
                    if not segmentation_read:
                        break
                    text_read += segmentation_read
                count += 1
            try:
                loads = json.loads(text_read)
                logger.info(loads)
                return loads
            except ValueError:
                logger.error("File content is not in json format")
                return None
    except Exception as e:
        logger.exception(
            "[Read file] catch an exception: %s" % e)
        return None


def is_pod_exist(api_instance, name, namespace, depth=0) -> bool:
    if depth == 5:
        logging.error("Kubernetes api failed too many times.")
        exit(1)
    try:
        resp = api_instance.list_namespaced_pod(namespace=namespace)
        name_list = [pod.metadata.name for pod in resp.items]
        return name in name_list
    except ApiException as e:
        if e.status != 404:
            logging.error("Unknown error: %s" % e)
            exit(1)
        else:
            logging.warning("kubernetes.client.exceptions.ApiException: %s" % e)
            return is_pod_exist(api_instance, name, namespace, depth + 1)


def is_pod_running(api_instance, name, namespace) -> bool:
    if not is_pod_exist(api_instance, name, namespace):
        logging.error("Pod %s does not exist in %s" % (name, namespace))
        return False
    try:
        resp = api_instance.read_namespaced_pod(name=name, namespace=namespace)
        return resp.status.phase == "Running"
    except ApiException as e:
        if e.status != 404:
            logging.error("Unknown error: %s" % e)
            exit(1)
        else:
            logging.warning("kubernetes.client.exceptions.ApiException: %s" % e)
            return False


def gen_template(instance, replicas):
    if replicas == 1:
        single = "true"
    else:
        single = "false"
    return {
        'apiVersion': 'apps/v1',
        'kind': 'StatefulSet',
        'metadata': {
            'name': instance,
            'namespace': NAMESPACE,
            'annotations': {
                'replicas': str(replicas)
            }
        },
        'spec': {
            'selector': {
                'matchLabels': {
                    'app': 'gauss'
                }
            },
            'serviceName': 'gauss-svc',
            'replicas': replicas,
            'template': {
                'metadata': {
                    'labels': {
                        'app': 'gauss'
                    }
                },
                'spec': {
                    'hostNetwork': True,
                    'terminationGracePeriodSeconds': 10,
                    'nodeSelector': {
                        'cloudsop/agent-type': 'Base'
                    },
                    'containers': [
                        {
                            'command': ['/bin/bash', '-c'],
                            'args': ['source ~/.bashrc && bash ./data/init.sh'],
                            'env': [
                                {
                                    'name': 'GAUSS_ROOT',
                                    'value': GAUSS_ROOT
                                },
                                {
                                    'name': 'SINGLE',
                                    'value': single
                                },
                                {
                                    'name': 'INSTANCE_ID',
                                    'valueFrom': {
                                        'fieldRef': {
                                            'fieldPath': 'metadata.name'
                                        }
                                    }
                                },
                                {
                                    'name': 'INSTANCE_IP',
                                    'valueFrom': {
                                        'fieldRef': {
                                            'fieldPath': 'status.podIP'
                                        }
                                    }
                                },
                            ],
                            'name': instance,
                            'image': 'qqq:latest',
                            'imagePullPolicy': 'Never',
                            # 'ports': [{'containerPort': 80, 'name': 'default'}],
                            'volumeMounts': [
                                {
                                    'name': 'gauss-claim',
                                    'mountPath': '/opt/data1/greenopengauss/data'
                                }
                            ]
                        }
                    ]
                }
            },
            'volumeClaimTemplates': [
                {
                    'metadata': {'name': 'gauss-claim'},
                    'spec': {
                        'accessModes': ['ReadWriteMany'],
                        'storageClassName': 'local-storage',
                        'resources': {
                            'requests': {'storage': '10Gi'}
                        }
                    }
                }
            ]
        }
    }


def is_sts_exist(api_instance, name, namespace, depth=0) -> bool:
    if depth == 5:
        logging.error("Kubernetes api failed too many times.")
        exit(1)
    try:
        resp = api_instance.list_namespaced_stateful_set(namespace=namespace)
        name_list = [sts.metadata.name for sts in resp.items]
        return name in name_list
    except ApiException as e:
        if e.status != 404:
            logging.error("Unknown error: %s" % e)
            exit(1)
        else:
            logging.warning("kubernetes.client.exceptions.ApiException: %s" % e)
            return is_sts_exist(api_instance, name, namespace, depth + 1)


def is_sts_ready(api_instance, name, namespace) -> bool:
    if not is_sts_exist(api_instance, name, namespace):
        logging.error("StatefulSet %s does not exist in %s" % (name, namespace))
        return False
    try:
        resp = api_instance.read_namespaced_stateful_set(name=name, namespace=namespace)
        return resp.status.ready_replicas is not None and resp.status.ready_replicas == resp.spec.replicas
    except ApiException as e:
        if e.status != 404:
            logging.error("Unknown error: %s" % e)
            exit(1)
        else:
            logging.warning("kubernetes.client.exceptions.ApiException: %s" % e)
            return False


def create_sts(api_instance, instance, replicas):
    sts = gen_template(instance, replicas)
    if is_sts_exist(api_instance, instance, NAMESPACE):
        logging.error("Sts %s exists in %s" % (instance, NAMESPACE))
        return None, FAILED
    try:
        resp = api_instance.create_namespaced_stateful_set(NAMESPACE, sts)
        logging.info("Create sts %s in %s, sts readyReplicas: %s" % (instance, NAMESPACE, resp.status.ready_replicas))
        return resp, SUCCESS
    except ApiException as e:
        if e.status != 404:
            logging.error("Unknown error: %s" % e)
            exit(1)
        else:
            logging.warning("kubernetes.client.exceptions.ApiException: %s" % e)
            return None, FAILED


def delete_sts(api_instance, instance):
    if not is_sts_exist(api_instance, instance, NAMESPACE):
        logging.error("Sts %s does not exist in %s" % (instance, NAMESPACE))
        return None, FAILED
    try:
        resp = api_instance.delete_namespaced_stateful_set(instance, NAMESPACE)
        logging.info("Delete sts %s from %s, action status: %s" % (instance, NAMESPACE, resp.status))
        return resp, SUCCESS
    except ApiException as e:
        if e.status != 404:
            logging.error("Unknown error: %s" % e)
            exit(1)
        else:
            logging.warning("kubernetes.client.exceptions.ApiException: %s" % e)
            return None, SUCCESS


def scale_sts(api_instance, instance, replicas):
    # # 为什么是这么写的？而不是read
    # body = gen_template(instance, replicas)
    # body['spec']['replicas'] = replicas
    if not is_sts_exist(api_instance, instance, NAMESPACE):
        logging.error("Sts %s does not exist in %s" % (instance, NAMESPACE))
        return None, FAILED
    try:
        # 改为read
        body = api_instance.read_namespaced_stateful_set(instance, NAMESPACE)
        body.spec.replicas = replicas
        
        resp = api_instance.patch_namespaced_stateful_set(instance, NAMESPACE, body)
        logging.info("Scale sts %s to %s, now the replicas is: %s" % (instance, replicas, resp.spec.replicas))
        return resp, SUCCESS
    except ApiException as e:
        if e.status != 404:
            logging.error("Unknown error: %s" % e)
            exit(1)
        else:
            logging.warning("kubernetes.client.exceptions.ApiException: %s" % e)
            return None, FALSE


class GaussHelper:
    def __init__(self, password, port):
        self.password = password
        self.port = port
        self.conn = None

    def exec_sql(self, sql, fetch=False):
        result = None
        self.get_connection()
        if not self.conn:
            logger.error("[ERROR]Incorrect password")
            return False, ""
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                if fetch:
                    result = cur.fetchall()
        except Exception as e:
            logger.error("exec sql(%s) failed: %s" % (sql, e))
            return False, result
        finally:
            self.close(cur)
        return True, result

    def get_connection(self):
        try:
            self.conn = connect(dbname="postgres", user=USER, password=self.password, host="127.0.0.1", port=self.port)
        except Exception as e:
            logger.error("[ERROR]Failed to initialize the connection")

    def close(self, cur):
        try:
            cur.close()
        except Exception:
            logger.error("Close gauss cursor failed")
        try:
            self.conn.close()
        except Exception:
            logger.error("Close gauss connect failed")


def main():
    logger.info("opengauss_ctl Go")
    result, status = exec_cmd("whoami")
    global USER
    USER = result[0]
    if USER == "root":
        logger.error("[ERROR]can not install openGauss with root")
        return FAILED
    command_service = {
        "create-db-instance": CreateDBInstance,
        "delete-db-instance": DeleteDBInstance,
        "add-database": AddDatabase,
        "remove-database": RemoveDatabase,
        "start-db-instance": StartDBInstance,
        "stop-db-instance": StopDBInstance
    }
    if len(sys.argv) == 1:
        print_screen(usage_info)
        return FAILED
    command = sys.argv[COMMAND_INDEX]
    args = sys.argv[ARG_START_INDEX:]
    args_map = _get_args_map(args)
    service = command_service.get(command)
    if not service:
        logger.error("[ERROR]the command is invalid: %s" % command)
        print_screen(usage_info)
        return FAILED
    return service().exec(args_map) == SUCCESS


if __name__ == '__main__':
    main()

