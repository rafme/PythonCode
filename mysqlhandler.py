import pymysql
import ConfigParser
import logging
from pymysql import DataError
from pymysql import ProgrammingError
from pprint import pprint


class mySQLHandler():
    """
    Logging handler for MySQL.

    Prerequisite: a config file containing the mySQL connection info

    Usage:

    import mySQLHandler
    mysqlh = mySQLHandler.mySQLHandler('testlog','../mt-cea/etl/common/ENV.mySQL.properties')
    mysqlh.db_log('ops_jobstat',action,record)
    """

    insert_ops_jobstat_sql = ("INSERT INTO <<DB_NAME>>.ops_jobstat("
                                 "    feed,"
                                 "    jobpid,"
                                 "    filename,"
                                 "    process_start,"
                                 "    process_end,"
                                 "    linecount,"
                                 "    rowcount,"
                                 "    filedate,"
                                 "    status,"
                                 "    message,"
                                 "    raw_size,"
                                 "    raw_replica_size,"
                                 "    refined_size,"
                                 "    refined_replica_size"
                                 "      )"
                                 "    VALUE("
                                 "     '%(feed)s',"
                                 "      %(jobpid)d,"
                                 "     '%(filename)s',"
                                 "     '%(process_start)s',"
                                 "     '%(process_end)s',"
                                 "     '%(linecount)s',"
                                 "     '%(rowcount)s',"
                                 "     '%(filedate)s',"
                                 "     '%(status)s',"
                                 "     '%(message)s',"
                                 "     '%(raw_size)s',"
                                 "     '%(raw_replica_size)s',"
                                 "     '%(refined_size)s',"
                                 "     '%(refined_replica_size)s'"
                                 "    );"
                                 "    ")

    update_ops_jobstat_sql = ("UPDATE <<DB_NAME>>.ops_jobstat "
                              "    set filename='%(filename)s',"
                              "        process_start='%(process_start)s',"
                              "        process_end='%(process_end)s',"
                              "        linecount='%(linecount)s',"
                              "        rowcount='%(rowcount)s',"
                              "        filedate='%(filedate)s',"
                              "        status='%(status)s',"
                              "        message='%(message)s',"
                              "        raw_size='%(raw_size)s',"
                              "        raw_replica_size='%(raw_replica_size)s',"
                              "        refined_size='%(refined_size)s',"
                              "        refined_replica_size='%(refined_replica_size)s'"
                              "    WHERE (feed = '%(feed)s' AND jobpid = '%(jobpid)d' );"
                              "    ")

    def __init__(self, configpath):
        """
        Constructor
        @param db: ['host','port','dbuser', 'dbpassword', 'dbname']
        @return: mySQLHandler
        """

        Config = ConfigParser.ConfigParser()
        Config.read(configpath)
        self.db = {'host': Config.get('mySQL', 'dbhost'),
              'port': int(Config.get('mySQL', 'port')),
              'dbuser': Config.get('mySQL', 'dbuser'),
              'dbpassword': Config.get('mySQL', 'password'),
              'schema': Config.get('mySQL', 'schema')}


    def perform_DML(self, arg_action, arg_record):
        l_DML = self.insert_ops_jobstat_sql if arg_action == 'insert' else self.update_ops_jobstat_sql
        l_DML = l_DML.replace('<<DB_NAME>>', self.db['schema'])
        l_DML = l_DML % arg_record

        # ------------------------ Make Connection ------------------------
        try:
            conn = pymysql.connect(host=self.db['host'],
                                   port=self.db['port'],
                                   user=self.db['dbuser'],
                                   passwd=self.db['dbpassword'],
                                   db=self.db['schema'])
        except Exception as e:
            pprint("ERROR : mySQLHandler.py: The Exception during db.connect: " + str(e))

        # ------------------------ Make Connection ------------------------
        cur = conn.cursor()
        try:
            #cur.execute(l_DML, arg_record)
            cur.execute(l_DML)
        except ProgrammingError as e:
            conn.rollback()
            cur.close()
            conn.close()
            pprint("ERROR : mySQLHandler.py: Exception programming during sql execute " + str(e))
        except Exception as e:
            conn.rollback()
            cur.close()
            conn.close()
            pprint("ERROR : mySQLHandler.py: Exception other during sql execute " + str(e))
        else:
            conn.commit()
        finally:
            cur.close()
            if conn.open:
                conn.close()
