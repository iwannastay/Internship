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

NAMESPACE = 'default'
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
        self.instance = None
        self.instance_dir = None

    def get_id(self):
        return self.instance + '-0'


class CreateDBInstance(Instance):
    def __init__(self):
        super().__init__()
        self.gs_initdb_path = GS_INITDB_PATH
        self.port = None
        self.password = "Changeme_123"
        self.default_port = 5432
        self.mandatory = ["instance"]
        self.optional = ["port", "mode"]
        self.instance = None

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return False
        instance = args_map.get("instance").strip()
        if not instance:
            logger.error("[ERROR]instance(%s) can't be blank" % instance)
            return False
        self.instance = instance
        port = args_map.get("port", self.default_port)
        if not isinstance(port, int) or int(port) > 65535:
            logger.error("[ERROR]port(%s) is not an integer or greater than 65535" % port)
            return False
        self.port = port

        self.instance_dir = GS_DATA_PATH + "/" + self.instance
        return True

    # @staticmethod
    # def is_exists(file):
    #     result, status = exec_remote_cmd("ls %s" % file)
    #     if status != 0:
    #         return False
    #     return True
    # 
    # @staticmethod
    # def is_dir(_dir):
    #     result, status = exec_remote_cmd("[[ -d %s ]]" % _dir)
    #     if status != 0:
    #         return False
    #     return True
    # 
    # @staticmethod
    # def is_dir_empty(_dir):
    #     result, status = exec_remote_cmd("ls -A %s" % _dir)
    #     if len(result) != 0:
    #         return False
    #     return True
    # 
    # def is_port_occupied(self, instance, namespace, port):
    #     result, status = exec_remote_cmd("netstat -tuln | grep %s" % port
    #                                      , self.core_v1, instance, namespace)
    #     if len(result) != 0:
    #         logger.error("[ERROR]port(%s)has been occupied" % port)
    #         return True
    #     return False

    def gen_template(self):
        return {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': self.instance
            },
            'spec': {
                'hostNetwork': True,
                'containers': [{
                    'image': 'qqq:latest',
                    'imagePullPolicy': 'IfNotPresent',
                    'name': self.instance,
                    "command": ["/bin/bash", "-c"],
                    "args": ["while true;do sleep 5; done"]
                }]
            }
        }

    def exec(self, args_map):
        file_dict = get_file(args_map.get("file"))
        if not file_dict:
            return FAILED
        if not self.check_param(file_dict):
            return FAILED
        return self.create_db_instance()

    # def check_install(self):
    #     if self.is_exists(self.gauss_home):
    #         return True
    #     return False
    #
    # def check_install_env(self, args_map):
    #     instance = args_map.get("instance")
    #     instance_dir = os.path.join(self.gauss_root, "data", instance)
    #     if self.is_exists(instance_dir) and not self.is_dir(instance_dir):
    #         logger.error("[ERROR]The target(%s) exists and is not a directory." % instance_dir)
    #         return False
    #     if self.is_dir(instance_dir) and not self.is_dir_empty(instance_dir):
    #         logger.error("[ERROR]The directory(%s) is not empty." % instance_dir)
    #         return False
    #     self.instance_dir = instance_dir
    #     port = args_map.get("port")
    #     if self.is_port_occupied(instance, NAMESPACE, port):
    #         return False
    #     self.port = port
    #     return True
    #
    # def prepare_pkg(self):
    #     if not self.check_install():
    #         pkg_path, status = exec_cmd("ls %s" % os.path.join(self.gauss_root, "openGauss*.tar.gz"))
    #         if status != 0:
    #             logger.error(
    #                 "[ERROR]The Gauss software package cannot be found in the following path(%s)" % self.gauss_root)
    #             return False
    #         if len(pkg_path) != 1:
    #             logger.error("[ERROR]More than one GaussDB software package exists in the following path(%s)")
    #             return False
    #         exec_cmd("mkdir -p %s" % self.gauss_home)
    #         result, status = exec_cmd("tar -zxvf %s -C %s" % (pkg_path[0], self.gauss_home))
    #         if status != 0:
    #             logger.error("[ERROR]Failed to decompress the Gauss software package: %s" % "".join(result))
    #             return False
    #     return True

    def create_pod(self, **kwargs):
        pod = self.gen_template()
        if is_pod_exist(self.core_v1, self.instance, NAMESPACE):
            logging.error("Pod %s exists in %s" % (self.instance, NAMESPACE))
            return None, FAILED
        try:
            resp = self.core_v1.create_namespaced_pod(NAMESPACE, pod)
            logging.info("Create pod %s in %s, pod status: %s" % (self.instance, NAMESPACE, resp.status.phase))
            return resp, SUCCESS
        except ApiException as e:
            if e.status != 404:
                logging.error("Unknown error: %s" % e)
                exit(1)
            else:
                logging.warning("kubernetes.client.exceptions.ApiException: %s" % e)
                return None, FAILED

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
        instance = self.instance
        instance_id = self.get_id()
        if is_sts_exist(self.app_v1, instance, NAMESPACE):
            logger.error("[INFO]instance(%s) is running" % instance)
            return FAILED

        logger.info("[INFO]Launching sts (%s)" % instance)
        _, ret = create_sts(self.app_v1, instance)
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
            self.clear_instance()
            return FAILED

        logger.info("Sts %s RUNNING" % instance)

        logger.info("[INFO]init instance")
        initdb_cmd = "%s -w %s -D %s --nodename 'sgnode' --locale='en_US.UTF-8'" % (
            self.gs_initdb_path, self.password, self.instance_dir)
        logger.info(initdb_cmd)
        result, status = exec_remote_cmd(initdb_cmd, self.core_v1, instance_id, NAMESPACE)
        result_txt = "\n".join(result)
        if re.search("error|Error", result_txt) is None and re.search("Success", result_txt) is not None:
            logger.info("[INFO]init instance(%s) success" % instance)
        else:
            logger.error("[ERROR]init instance(%s) failed: %s" % (instance, result))
            self.clear_instance()
            return FAILED

        config_cmd = "echo \"host all all 0.0.0.0/0 sha256\" >> {2} \
            && sed -i \"/^#listen_addresses =/c\\listen_addresses = '*'\" {1} \
            && sed -i '/^#port =/c\\port = {0}' {1}".format(
            self.port,
            os.path.join(self.instance_dir + "/postgresql.conf"),
            os.path.join(self.instance_dir + "/pg_hba.conf"))
        logger.info(config_cmd)
        _, ret = exec_remote_cmd(config_cmd, self.core_v1, instance_id, NAMESPACE)
        if ret:
            logger.error("[ERROR]config instance(%s) failed: %s" % (instance, result))
            self.clear_instance()
            return FAILED

        logger.info("[INFO]start instance")
        startdb_cmd = "%s start -D %s -Z single_node" % (self.gs_ctl_path, self.instance_dir)
        logger.info(startdb_cmd)
        result, status = exec_remote_cmd(startdb_cmd, self.core_v1, instance_id, NAMESPACE)
        if re.search("server started", "\n".join(result)) is not None:
            logger.info("[INFO]create instance(%s) success" % instance)
        else:
            logger.error("[ERROR]create instance(%s) failed" % instance)
            self.clear_instance()
            return FAILED
        return SUCCESS


class DeleteDBInstance(Instance):
    def __init__(self):
        super().__init__()
        self.mandatory = ["instance"]
        self.instance = None

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return False
        self.instance = args_map.get("instance")
        self.instance_dir = GS_DATA_PATH + "/" + self.instance
        return True

    def delete_pod(self, **kwargs):
        instance = self.instance
        if not is_pod_exist(self.core_v1, instance, NAMESPACE):
            logging.error("Pod %s does not exist in %s" % (instance, NAMESPACE))
            return None, FAILED
        try:
            resp = self.core_v1.delete_namespaced_pod(instance, NAMESPACE)
            logging.info("Delete pod %s from %s, pod status: %s" % (instance, NAMESPACE, resp.status.phase))
            return resp, SUCCESS
        except ApiException as e:
            if e.status != 404:
                logging.error("Unknown error: %s" % e)
                exit(1)
            else:
                logging.warning("kubernetes.client.exceptions.ApiException: %s" % e)
                return None, SUCCESS

    def exec(self, args_map):
        instance = args_map.get("instance")
        if not self.check_param(args_map):
            return FAILED

        _, status = delete_sts(self.app_v1, instance)
        if status != 0:
            logger.error("[ERROR]delete instance(%s) failed" % instance)
            return FAILED
        else:
            rmdb_cmd = "rm -rf /home/dbuser/gaussdata/%s" % instance  # self.instance_dir
            logger.info(rmdb_cmd)
            _, status = exec_cmd(rmdb_cmd)
            if status != 0:
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

        
    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return False
        self.instance = args_map.get("instance")
        self.instance_dir = GS_DATA_PATH + "/" + self.instance
        instance_id = self.get_id()
        if not is_sts_exist(self.app_v1, self.instance, NAMESPACE):
            logger.error("[ERROR]Sts (%s) must exist" % self.instance)
            return False
        result, status = exec_remote_cmd(
            "ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep | awk '{print $2}'" % self.instance
            , self.core_v1, instance_id, NAMESPACE)
        if len(result) == 0:
            logger.error("[ERROR]instance(%s) has not been created or started" % self.instance)
            return False
        pid = int(result[0])
        result, status = exec_remote_cmd(
            "sed -n '/^port/p' %s/postgresql.conf | awk '{print $3}'" % self.instance_dir,
            self.core_v1, instance_id, NAMESPACE)
        self.port = result[-1]
        print("asdasdasd", result, self.port)
        dbnames = args_map.get("dbnames")
        if dbnames is not None:
            str_list = dbnames.split(',')
            if len(str_list) == 0:
                logger.error("[ERROR]dbnames(%s) can't be empty" % dbnames)
                return False
            for item in str_list:
                if not str(item):
                    logger.error("[ERROR]dbnames(%s) is invalid!" % dbnames)
                    return False
            self.dbname_list = str_list
        self.password = "Changeme_123"
        return True

    def exec_sql(self, sql, fetch=False):
        result = None
        print(self.password, self.port)
        conn = get_connection(self.password, self.port)
        if not conn:
            logger.error("[ERROR]Incorrect password")
            return False, None
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                if fetch:
                    result = cur.fetchall()
        except Exception as e:
            logger.error("exec sql(%s) failed: %s" % (sql, e))
            return False, result
        finally:
            self.close(cur, conn)
        return True, result

    @staticmethod
    def close(cur, conn):
        try:
            cur.close()
        except Exception:
            logger.error("Close gauss cursor failed")
        try:
            conn.close()
        except Exception:
            logger.error("Close gauss connect failed")

    def exec(self, args_map):
        if not self.check_param(args_map):
            return FAILED
        dbname_list = args_map.get("dbnames").split(',')
        print(dbname_list)
        ret, result = self.exec_sql("SELECT datname FROM pg_database", fetch=True)
        exist_dbnames = [item[0] for item in result]
        duplicate_dbnames = [item for item in dbname_list if item in exist_dbnames]
        if duplicate_dbnames:
            logger.error("[ERROR]The following database(%s) already exists" % duplicate_dbnames)
            return FAILED
        sql_add_database = {
            "01": "REVOKE ALL ON SCHEMA PUBLIC FROM PUBLIC",
            "02": "CREATE TABLESPACE {0} RELATIVE LOCATION 'tablespace/{0}' MAXSIZE '128M'",
            "03": "CREATE USER {0} ENCRYPTED Password '{1}' NOSYSADMIN NOINHERIT CONNECTION LIMIT 50 PERM SPACE '128M' TEMP SPACE '128M' SPILL SPACE '128M';",
            "04": "ALTER TABLESPACE {0} OWNER TO {0}",
            "05": "ALTER USER {0} SET search_path TO {0}",
            "06": "ALTER USER {0} SET default_tablespace TO {0}",
            "07": "CREATE DATABASE {0} WITH TABLESPACE = {0}"
        }
        for dbname in dbname_list:
            for item_key, item_value in sql_add_database.items():
                if item_key == "01":
                    ret, result = self.exec_sql(item_value)
                    if not ret:
                        logger.error("[ERROR]create database(%s) failed" % dbname)
                        return FAILED
                elif item_key == "03":
                    ret, result = self.exec_sql(item_value.format(dbname, self.password))
                    if not ret:
                        logger.error("[ERROR]create database(%s) failed" % dbname)
                        return FAILED
                else:
                    ret, result = self.exec_sql(item_value.format(dbname))
                    if not ret:
                        logger.error("[ERROR]create database(%s) failed" % dbname)
                        return FAILED
        logger.info("[INFO]create database(%s) success" % dbname_list)
        return SUCCESS


class RemoveDatabase(Instance):
    def __init__(self):
        super().__init__()
        self.mandatory = ["instance", "dbnames"]
        self.dbname_list = None
        self.password = None
        self.port = None

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return False
        self.instance = args_map.get("instance")
        self.instance_dir = GS_DATA_PATH + '/' + self.instance
        instance_id = self.get_id()
        result, status = exec_remote_cmd(
            "ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep | awk '{print $2}'" % self.instance
            , self.core_v1, instance_id, NAMESPACE)
        if len(result) == 0:
            logger.error("[ERROR]instance(%s) has not been created or started" % self.instance)
            return False
        pid = result[0]
        result, status = exec_remote_cmd(
            "sed -n '/^port/p' %s/postgresql.conf | awk '{print $3}'" % self.instance_dir,
            self.core_v1, instance_id, NAMESPACE)
        self.port = result[-1]
        dbnames = args_map.get("dbnames")
        if dbnames is not None:
            str_list = dbnames.split(',')
            if len(str_list) == 0:
                logger.error("[ERROR]dbnames(%s) can't be empty" % dbnames)
                return False
            for item in str_list:
                if not str(item):
                    logger.error("[ERROR]dbnames(%s) is invalid!" % dbnames)
                    return False
            self.dbname_list = str_list
        self.password = "Changeme_123"
        return True

    def exec_sql(self, sql, fetch=False):
        result = None
        conn = get_connection(self.password, self.port)
        if not conn:
            logger.error("[ERROR]Incorrect password")
            return False
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                if fetch:
                    result = cur.fetchall()
        except Exception as e:
            logger.error("exec sql(%s) failed: %s" % (sql, e))
            return False, result
        finally:
            self.close(cur, conn)
        return True, result

    @staticmethod
    def close(cur, conn):
        try:
            cur.close()
        except Exception:
            logger.error("Close gauss cursor failed")
        try:
            conn.close()
        except Exception:
            logger.error("Close gauss connect failed")

    def exec(self, args_map):
        if not self.check_param(args_map):
            return FAILED
        dbname_list = args_map.get("dbnames").split(',')
        sql_add_database = {
            # "01": "ALTER TABLESPACE {db_name} OWNER TO {sys_user}",
            "02": "CLEAN CONNECTION TO ALL FORCE FOR DATABASE {0}",
            "03": "DROP DATABASE IF EXISTS {0}",
            "04": "DROP TABLESPACE IF EXISTS {0}",
            "05": "DROP USER IF EXISTS {0} CASCADE"
        }
        for dbname in dbname_list:
            for item_key, item_value in sql_add_database.items():
                ret, result = self.exec_sql(item_value.format(dbname))
                if not ret:
                    logger.error("[ERROR]remove database(%s) failed" % dbname)
                    return FAILED
        logger.info("[INFO]delete database(%s) success" % dbname_list)
        return SUCCESS


class StartDBInstance(Instance):
    def __init__(self):
        super().__init__()
        self.instance = None
        self.mandatory = ["instance"]

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return False
        instance = args_map.get("instance")

        self.instance = instance
        self.instance_dir = os.path.join(GS_DATA_PATH + "/" + instance)
        return True

    def exec(self, args_map):
        if not self.check_param(args_map):
            return FAILED
        logger.info("[INFO]start instance")
        return self.start_db_instance()

    def start_db_instance(self):
        instance = self.instance
        instance_id = self.get_id()
        if is_sts_exist(self.app_v1, instance, NAMESPACE):
            logger.warning("[INFO]Sts (%s) exists" % instance)
            # return FAILED
        else:
            logger.info("[INFO]Launching sts (%s)" % instance)
            _, ret = create_sts(self.app_v1, instance)
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
            delete_sts(self.app_v1, instance)
            return FAILED

        logger.info("Sts %s RUNNING" % instance)

        result, status = exec_remote_cmd("ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep" % instance
                                         , self.core_v1, instance_id, NAMESPACE)
        if len(result) > 0:
            logger.error("[INFO]instance(%s) is running" % instance)
            return FAILED

        startdb_cmd = "%s start -D %s -Z single_node" % (self.gs_ctl_path, self.instance_dir)
        logger.info(startdb_cmd)
        result, status = exec_remote_cmd(startdb_cmd, self.core_v1, instance_id, NAMESPACE)
        if re.search("server started", "\n".join(result)) is not None:
            logger.info("[INFO]start instance(%s) success" % instance)
        else:
            logger.error("[ERROR]start instance(%s) failed" % instance)
            return FAILED
        return SUCCESS
        # if not is_sts_exist(self.app_v1, instance, NAMESPACE):
        #     logger.error("[INFO]Sts (%s) has no pod running" % instance)
        #     return FAILED
        # result, status = exec_remote_cmd("ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep" % instance
        #                                  , self.core_v1, instance_id, NAMESPACE)
        # if len(result) > 0:
        #     logger.error("[INFO]instance(%s) is running" % instance)
        #     return FAILED
        #
        # startdb_cmd = "%s start -D %s -Z single_node" % (self.gs_ctl_path, self.instance_dir)
        # logger.info(startdb_cmd)
        # result, status = exec_remote_cmd(startdb_cmd, self.core_v1, instance_id, NAMESPACE)
        # if re.search("server started", "\n".join(result)) is not None:
        #     logger.info("[INFO]start instance(%s) success" % instance)
        # else:
        #     logger.error("[ERROR]start instance(%s) failed" % instance)
        #     return FAILED
        # return SUCCESS


class StopDBInstance(Instance):
    def __init__(self):
        super().__init__()
        self.instance = None
        self.mandatory = ["instance"]

    def check_param(self, args_map):
        for item in self.mandatory:
            if item not in args_map:
                logger.error("[ERROR]argument(%s) must exist" % item)
                return False
        instance = args_map.get("instance")
        self.instance = instance
        self.instance_dir = os.path.join(GS_DATA_PATH + "/" + instance)
        return True

    def exec(self, args_map):
        if not self.check_param(args_map):
            return FAILED
        logger.info("[INFO]stop instance")
        return self.stop_db_instance()

    def stop_db_instance(self):
        instance = self.instance
        instance_id = self.get_id()
        if not is_sts_ready(self.app_v1, instance, NAMESPACE):
            logger.error("[INFO]Sts (%s) has no pod running" % instance)
            return FAILED

        result, status = exec_remote_cmd("ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep" % instance
                                         , self.core_v1, instance_id, NAMESPACE)
        if len(result) == 0:
            logger.info("[INFO]instance(%s) is not running" % instance)
            return FAILED

        stopdb_cmd = "%s stop -D %s" % (self.gs_ctl_path, self.instance_dir)
        logger.info(stopdb_cmd)
        result, status = exec_remote_cmd(stopdb_cmd, self.core_v1, instance_id, NAMESPACE)
        if re.search("server stopped", "\n".join(result)) is not None:
            logger.info("[INFO]stop instance(%s) success" % instance)
        else:
            logger.warning("[ERROR]stop instance(%s) failed" % instance)
            # killdb_cmd = "ps -efww | grep -w %s | grep 'gaussdb ' | grep -v grep | " \
            #              "awk '{print $2}' | xargs kill -9" % instance
            # result, status = exec_remote_cmd(killdb_cmd, self.core_v1, instance_id, NAMESPACE)
            # if status == 0:
            #     logger.info("[INFO]stop instance(%s) success" % instance)
            # else:
            #     logger.error("[ERROR]stop instance(%s) failed" % instance)
            #     return FAILED

        # scale sts to 0
        _, status = scale_sts(self.app_v1, instance, 0)
        if status != 0:
            logger.error("[ERROR]Scale sts (%s) failed" % instance)
            # return FAILED
        else:
            logger.info("[INFO]Scale instance(%s) success" % instance)
            # return SUCCESS

        # or delete sts
        _, status = delete_sts(self.app_v1, instance)
        if status != 0:
            logger.error("[ERROR]delete sts (%s) failed" % instance)
            return FAILED
        else:
            logger.info("[INFO]delete instance(%s) success" % instance)
            return SUCCESS


def get_connection(password, port):
    try:
        conn = connect(dbname="postgres", user=USER, password=password, host="127.0.0.1", port=port)
        return conn
    except Exception as e:
        logger.error("[ERROR]Failed to initialize the connection")


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


def gen_template(instance):
    return {
        'apiVersion': 'apps/v1',
        'kind': 'StatefulSet',
        'metadata': {'name': instance},
        'spec': {
            'selector': {
                'matchLabels': {
                    'app': 'gauss'
                }
            },
            'serviceName': 'gauss-svc',
            'replicas': 1,
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
                            'args': ['source ~/.bashrc && bash ./data/init.sh.single'],
                            'env': [
                                {
                                    'name': 'GAUSS_ROOT',
                                    'value': GAUSS_ROOT
                                },
                                {
                                    'name': 'INSTANCE',
                                    'value': instance
                                }
                            ],
                            'name': instance,
                            'image': 'qqq:latest',
                            'imagePullPolicy': 'Never',
                            'ports': [{'containerPort': 80, 'name': 'default'}],
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
                        'accessModes': ['ReadWriteOnce'],
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


def create_sts(api_instance, instance):
    sts = gen_template(instance)
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
    body = gen_template(instance)
    body['spec']['replicas'] = replicas
    if not is_sts_exist(api_instance, instance, NAMESPACE):
        logging.error("Sts %s does not exist in %s" % (instance, NAMESPACE))
        return None, FAILED
    try:
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


def main():
    logger.info("opengauss_ctl Go")
    result, status = exec_cmd("whoami")
    USER = result[0]
    if USER == "root":
        logger.error("[ERROR]can not install openGauss with root")
        return FAILED
    CommandService = {
        "create-db-instance": CreateDBInstance,
        "delete-db-instance": DeleteDBInstance,
        "add-database": AddDatabase,
        "remove-database": RemoveDatabase,
        "start-db-instance": StartDBInstance,
        "stop-db-instance": StopDBInstance
    }
    command = sys.argv[COMMAND_INDEX]
    if command is None:
        print_screen(usage_info)
        return FAILED
    args = sys.argv[ARG_START_INDEX:]
    args_map = _get_args_map(args)
    service = CommandService.get(command)
    if not service:
        logger.error("[ERROR]the command is invalid: %s" % command)
        print_screen(usage_info)
        return FAILED
    return service().exec(args_map)


if __name__ == '__main__':
    main()

