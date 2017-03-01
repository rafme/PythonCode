# -*- coding: utf-8 -*-
"""
Created on 11/30/2016
"""

import pymysql
import ConfigParser
from collections import OrderedDict


class mySQLMetadata(object):
    """
    Retrieve metadata for table and columns for MySQL.

    Prerequisites: a db user

    Usage:

    import mySQLMetadata
    sqlMetadata = mySQLMetadata(config path Ex: '../mt-cea/etl/common/ENV.mySQL.properties')
    sqlMetadata.read_table_metadata(some-schema,some-table)
    """

    select_table_metadata_sql = """SELECT
      SOURCESCHEMA,
      SOURCETBLNM,
      REFRESHRATE,
      DELTACOLUMN,
      DELTACOLUMNTYPE,
      DELTACOLUMNFMT,
      DELTACOLUMNVAL,
      DB_TYPE,
      DB_SERVER,
      DB_USER,
      EXTRACTLANDINGDIR,
      HDFS_BASEDIR,
      HDFSEXTRACTDIR,
      DELIMITER,
      BUCKETINGCLAUSE,
      TGT_STG_DB,
      TGT_DB,
      HIVE_TBL_TRUNCATE,
      TRGTTBLNM,
      HIVE_RAW_RETENTION,
      HIVE_REFINED_RETENTION,
      HIVE_RAW_DIR,
      HIVE_REFINED_DIR,
      REFRESH_REFINED_DDL_DML,
      V1_SUPPORT,
      JPMISDAF_NUM,
      SOURCE_SQL_TXT,
      SPLIT_BY
    FROM dops.METADATA_FOR_TBL
    WHERE SOURCESCHEMA = %s AND SOURCETBLNM = %s """

    select_column_metadata_sql = """SELECT
      SOURCEDB,
      SOURCETABLE,
      SOURCECOLS,
      COLNAMEINHIVE,
      COLDATATYPEINHIVE,
      LENGTH,
      IsExtract,
      IsPartitioned,
      ColFormat,
      IsPII,
      Comment
    FROM dops.METADATA_FOR_COL
    WHERE SOURCEDB = %s AND SOURCETABLE = %s
    ORDER BY Seq_nb"""

    def __init__(self, configpath):
        """
        Constructor
        @return: mySQLMetadata
        """
        config = ConfigParser.ConfigParser()
        config.read(configpath)
        db = {'host': config.get('mySQL', 'dbhost'),
              'port': int(config.get('mySQL', 'port')),
              'dbuser': config.get('mySQL', 'dbuser'),
              'dbpassword': config.get('mySQL', 'password'),
              'dbname': 'dops'}
        self.db = db

    def read_table_metadata(self, sourceDB, sourceTable):
        conn = pymysql.connect(host=self.db['host'], port=self.db['port'], user=self.db['dbuser'],
                               passwd=self.db['dbpassword'], db=self.db['dbname'])
        cursor = conn.cursor()
        cursor.execute(mySQLMetadata.select_table_metadata_sql, (sourceDB, sourceTable))
        results = cursor.fetchall()
        tbl_metadata = {}
        if len(results) > 0:
            rs = results[0]
            tbl_metadata = {'src_db': rs[0].upper(),
                            'src_tbl': rs[1].upper(),
                            'rfrsh_rate': rs[2].upper(),
                            'delta_col': rs[3].upper(),
                            'delta_col_typ': rs[4].upper(),
                            'delta_col_fmt': rs[5].upper(),
                            'db_type': rs[7].upper(),
                            'db_server': rs[8].upper(),
                            'db_user': rs[9].upper(),
                            'extract_landing_dir': rs[10].lower(),
                            'hdfs_basedir': rs[11].lower(),
                            'hdfs_extract_dir': rs[12].lower(),
                            'stg_tbl_delimiter': rs[13] if rs[13] else '',
                            'tgt_tbl_bkt_clause': rs[14].upper(),
                            'stg_db': rs[15].upper(),
                            'tgt_db': rs[16].upper(),
                            'hive_tbl_truncate': rs[17].upper(),
                            'tgt_tbl': rs[18].upper(),
                            'hive_raw_retention': str(rs[19]) if rs[19] else '0',
                            'hive_refined_retention': str(rs[20]) if rs[20] else '0',
                            'hdfs_raw_dir': rs[21].lower(),
                            'hdfs_refined_dir': rs[22].lower(),
                            'refresh_refined_DDL_DML': rs[23].upper(),
                            'v1_support': rs[24].upper(),
                            'jpmisdaf_num': rs[25].upper() if rs[25] else '',
                            'source_sql_txt': rs[26] if rs[26] else '',
                            'split_by': rs[27] if rs[27] else ''        }
        else:
            print("No data found for " + sourceDB + "," + sourceTable)

        cursor.close()  # close current cursor
        conn.close()
        return tbl_metadata

    def readColumnMetadata(self, sourceDB, sourceTable):
        conn = pymysql.connect(host=self.db['host'], port=self.db['port'], user=self.db['dbuser'],
                               passwd=self.db['dbpassword'], db=self.db['dbname'])
        cursor = conn.cursor()
        cursor.execute(mySQLMetadata.select_column_metadata_sql, (sourceDB, sourceTable))
        results = cursor.fetchall()
        col_md_list = []
        if len(results) > 0:
            for rs in results:
                columnMetadata = OrderedDict()
                columnMetadata['SOURCEDB']          = rs[0]
                columnMetadata['SOURCETABLE']       = rs[1]
                columnMetadata['SOURCECOLS']        = rs[2].upper()
                columnMetadata['COLNAMEINHIVE']     = rs[3].upper()
                columnMetadata['COLDATATYPEINHIVE'] = rs[4].upper()
                columnMetadata['LENGTH']            = rs[5]
                columnMetadata['IsExtract']         = rs[6]
                columnMetadata['IsPartitioned']     = rs[7]
                columnMetadata['ColFormat']         = rs[8]
                columnMetadata['IsPII']             = rs[9]
                columnMetadata['Comment']           = rs[10]
                col_md_list.append(columnMetadata.items())
        else:
            print("No data found for " + sourceDB + "," + sourceTable)

        cursor.close()  # close current cursor
        conn.close()
        return col_md_list

