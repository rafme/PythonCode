from __future__ import print_function
from datetime import datetime
from os.path import isfile
from getpass import getuser
from commands import getstatusoutput
from mySQLHandler import mySQLHandler
#import mySQLHandler_test as  mySQLHandler
import csv
import socket
import sys
import os
import errno
import base64
import logging
import ConfigParser


class GlobalValues:
    extract_file = ''
    extract_log = ''
    extract_count = 0
    record = dict(feed='', jobpid=0, filename='', process_start=datetime.now(), process_end=datetime.now(), linecount=0,
                  rowcount=0, filedate='', status='', message='', action='insert', dbtable='ops_jobstat', raw_size='', raw_replica_size= '',
                 refined_size= '', refined_replica_size = '')
    mySQLHandler_instance = None
    epv_aim_file = ''

class MsgColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

mySQLhdr = None

def abort_with_msg(msg):
    ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(MsgColors.FAIL + "\n" + ts + ":\tERROR: " + msg + "\n" + MsgColors.ENDC)

    record = GlobalValues.record
    # The below condition is to skip the logging in mySQL until the Generate script part begins
    if record['feed'] != '' and record['jobpid'] > 0:
        record['process_end'] = datetime.now()
        record['status'] = "FAILED WITH ERROR"
        record['message'] = ts + ": ERROR: " + msg.replace("'","")
        GlobalValues.mySQLHandler_instance.perform_DML('update', record)

    logging.debug(MsgColors.FAIL + "\n" + ts + ":\tERROR: " + msg + "\n" + MsgColors.ENDC)
    sys.exit(-1)

def print_warn(msg):
    ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(MsgColors.WARNING + "\n" + ts + ":\tWARNING: " + msg + "\n" + MsgColors.ENDC)
    logging.warning(MsgColors.WARNING + "\n" + ts + ":\tWARNING: " + msg + "\n" + MsgColors.ENDC)


def print_info(msg):
    ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    print(ts + ":\tINFO: " + msg)
    logging.info(ts + ":\tINFO: " + msg)


def search_log(arg_logfile, arg_search_str):
    with open(arg_logfile) as logfile:
        for line in logfile:
            if arg_search_str in line:
                return True, line
            else:
                return False, ''


def dir_check_create(full_file_path):
    """
    :param full_file_path: The full file path for which the directory check will be performed
    :rtype: 0 for success, -1 if errored
    :returns : None
    """
    if not os.path.exists(os.path.dirname(full_file_path)):
        try:
            os.makedirs(os.path.dirname(full_file_path))
        except OSError as exc:  # Guard against race condition
            print_warn("Necessary temp directories could not get created " + full_file_path)
            if exc.errno != errno.EEXIST:
                raise


def get_passwd(arg_user, arg_passwd_file):
    passwd = ""
    data_reader = csv.reader(open(arg_passwd_file, 'rt'), delimiter=':', quotechar='"', quoting=csv.QUOTE_ALL)
    for rec in data_reader:
        if len(rec) != 0 and rec[0] == arg_user:
            passwd = base64.b64decode(rec[1]).replace('\n', '').replace('\r', '')

    data_reader = csv.reader(open(GlobalValues.epv_aim_file, 'rt'), delimiter=':', quotechar='"', quoting=csv.QUOTE_ALL)
    for rec in data_reader:
        if len(rec) != 0 and rec[0] == arg_user:
            passwd = getstatusoutput('/opt/CARKaim/sdk/clipasswordsdk getpassword ' +
                                     '-p AppDescs.AppID=87200-E-000 ' +
                                     '-p Query="' + rec[1] + '" -o Password')[1]
    if passwd == "":
        abort_with_msg("Password for ID:" + arg_user + ", not found in properties file")
    else:
        return passwd


def check_if_running(arg_process_name):
    global lock_socket  # Without this our lock gets garbage collected
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + arg_process_name)
        print_info("Checking if script is already running")
    except socket.error:
        abort_with_msg("Process " + arg_process_name + " is already running, only one instance is allowed")
        sys.exit()


def read_file(arg_db_nm, arg_tbl_nm, arg_input_file, arg_content_type):
    """
    :
    :param arg_db_nm: The source database name. Used to identify the row in Metadata
    :param arg_tbl_nm: The source tablee name. Also used to identify the row in Metadata
    :param arg_input_file: The Table or Column level Metadata file
    :param arg_content_type: Indicates whether file passed is Table or Column level Metadata file

    :rtype: 0 for success, -1 if errored
    :returns : A dictionary of Table level Metadata or a List of Table Columns to extract and load
    """
    try:
        data_reader = csv.reader(open(arg_input_file, 'rt'),
                                 delimiter='\t',
                                 quotechar='"',
                                 quoting=csv.QUOTE_ALL)
        next(data_reader)  # To skip the header row
        try:
            meta_key_value = {}
            value_list = []

            for rec in data_reader:
                if len(rec) != 0 and rec[0].upper() == arg_db_nm and rec[1].upper() == arg_tbl_nm:
                    if arg_content_type == 'TABLES':  # Each Table has a single row of Metadata
                        meta_key_value["src_db"] = rec[0].upper()
                        meta_key_value["src_tbl"] = rec[1].upper()
                        meta_key_value["rfrsh_rate"] = rec[2].upper()
                        meta_key_value["delta_col"] = rec[3].upper()
                        meta_key_value["delta_col_typ"] = rec[4].upper()
                        meta_key_value["delta_col_fmt"] = rec[5].upper()
                        meta_key_value["delta_col_val"] = rec[6].upper()
                        meta_key_value["db_type"] = rec[7].upper()
                        meta_key_value["db_server"] = rec[8].upper()
                        meta_key_value["db_user"] = rec[9].upper()
                        meta_key_value["extract_landing_dir"] = rec[10].lower()
                        meta_key_value["hdfs_basedir"] = rec[11].lower()
                        meta_key_value["hdfs_extract_dir"] = rec[12].lower()
                        meta_key_value["stg_tbl_delimiter"] = rec[13] if rec[13] else '\\001'
                        meta_key_value["tgt_tbl_bkt_clause"] = rec[14].upper()
                        meta_key_value["stg_db"] = rec[15].upper()
                        meta_key_value["tgt_db"] = rec[16].upper()
                        meta_key_value["hive_tbl_truncate"] = rec[17].upper()
                        meta_key_value["tgt_tbl"] = rec[18].upper()
                        meta_key_value["hive_raw_retention"] = rec[19]      # Used in ops_gen_scripts, not here
                        meta_key_value["hive_refined_retention"] = rec[20]  # Used in ops_gen_scripts, not here
                        meta_key_value["hdfs_raw_dir"] = rec[21].lower()
                        meta_key_value["hdfs_refined_dir"] = rec[22].lower()
                        meta_key_value["refresh_refined_DDL_DML"] = rec[23].upper() if rec[23] else 'N'
                        meta_key_value["v1_support"] = rec[24].upper() if rec[23] else 'N'  # Support framework V1
                    elif arg_content_type == 'COLS' and rec[6].upper() == 'Y':  # If Extract Flag = Y, get below
                        value_list.append([rec[2].upper(),  # SourceColumns
                                           rec[3].upper(),  # ColumnInHive
                                           rec[4].upper(),  # ColumnDataTypeInHive
                                           rec[5],  # Length
                                           rec[7],  # IsPartitioned (Y/N)
                                           rec[8],  # ColFmt
                                           rec[9]])  # Is PII (Y/N)
            if arg_content_type == 'COLS':
                if len(value_list) == 0:
                    abort_with_msg("Could not find Column Metadata for : " + arg_db_nm + "." + arg_tbl_nm)
                else:
                    print_info("SourceColumns, ColumnInHive, ColumnDataTypeInHive, Length, "
                               "IsPartitioned, ColFmt, IsPII, Comment")
                    for col_meta_row in value_list:
                        print_info("\t\t" + str(col_meta_row))
                    return value_list

            if arg_content_type == 'TABLES':
                if len(meta_key_value) == 0:
                    abort_with_msg("Could not find Table Metadata for : " + arg_db_nm + "." + arg_tbl_nm)
                else:
                    print_info("Table Metadata Read ")
                    for key, val in sorted(meta_key_value.items()):
                        print_info("\t\t" + key + " : " + val)
                    return meta_key_value
        except Exception as err:
            abort_with_msg(str(err) + ": When reading file " + arg_input_file)
    except IOError as err:
        abort_with_msg(str(err) + ": When reading file " + arg_input_file)


def write_content(arg_output_file, arg_contents, arg_overwrite_flag):
    try:
        if arg_overwrite_flag == 'N':
            if isfile(arg_output_file):
                print_warn("File " + arg_output_file + " already exists. Will not be overwritten")
            else:
                print_info("Generating :" + arg_output_file)
                with open(arg_output_file, 'wb') as file_write_handle:
                    file_write_handle.write(arg_contents)
        if arg_overwrite_flag == 'Y':
            print_info("Generating :" + arg_output_file)
            with open(arg_output_file, 'wb') as file_write_handle:
                file_write_handle.write(arg_contents)
        if arg_overwrite_flag == 'A':
            print_info("Appending to :" + arg_output_file)
            with open(arg_output_file, 'a') as file_write_handle:
                file_write_handle.write(arg_contents)
    except IOError as err:
        abort_with_msg(str(err) + ": When writing to file " + arg_output_file)


def build_extract_filter(arg_tbl_meta, arg_from_val, arg_to_val, arg_operator, run_switch):
    """
    :
    :param arg_tbl_meta: The array of table attributes fetched from metadata
    :param arg_from_val: Most likely a date value used to pull incremental data from source
    :param arg_to_val: Most likely a date value, used to end a range of dates for an incremental pull
    :param arg_operator: Possible values: >, >=, <, <=, =, <> used to build the condition
    :rtype 0 for success, -1 if errored
    :return a string used by the extractor
    """

    l_test_condition = arg_operator
    l_extract_filter = ""
    l_start_val = arg_from_val
    l_end_val = arg_to_val

    if arg_to_val is None:
        if arg_tbl_meta["db_type"].__contains__('TERADATA'):
            # Teradata PT needs 2 single quotes
            l_extract_filter = arg_tbl_meta['delta_col'] + l_test_condition + \
                               "CAST(''" + l_start_val + \
                               "'' AS " + arg_tbl_meta['delta_col_typ'] + \
                               " FORMAT ''" + arg_tbl_meta['delta_col_fmt'] + "'')"
            print_info("TPT  Filter: {0}".format(l_extract_filter))
            l_extract_feed_filter = "=  CAST(''" + l_start_val + \
                               "'' AS " + arg_tbl_meta['delta_col_typ'] + \
                               " FORMAT ''" + arg_tbl_meta['delta_col_fmt'] + "'')"
            print_info("TPT  Filter: {0}".format(l_extract_feed_filter))
        elif arg_tbl_meta["db_type"] == 'ORACLE' and arg_tbl_meta['delta_col'] is not None:
            l_extract_filter = arg_tbl_meta['delta_col'] + l_test_condition + \
                               "CAST('" + l_start_val + "' AS " + arg_tbl_meta['delta_col_typ'] + ")"
            print_info("SQLPlus Filter: {0}".format(l_extract_filter))
            l_extract_feed_filter = "=  CAST('" + l_start_val + "' AS " + arg_tbl_meta['delta_col_typ'] + ")"
            print_info("SQLPlus Filter: {0}".format(l_extract_feed_filter))
        else:
            print_info("Currently only Teradata and Oracle sources are supported for filtering")
    else:
        l_test_condition = " BETWEEN "
        if arg_tbl_meta["db_type"].__contains__('TERADATA'):  # Teradata PT needs 2 single quotes
            l_extract_filter = arg_tbl_meta['delta_col'] + l_test_condition + \
                               "CAST(''" + l_start_val + "'' AS " + arg_tbl_meta['delta_col_typ'] + \
                               " FORMAT ''" + arg_tbl_meta['delta_col_fmt'] + "'')" + \
                               " AND " + \
                               "CAST(''" + l_end_val + "'' AS " + arg_tbl_meta['delta_col_typ'] + \
                               " FORMAT ''" + arg_tbl_meta['delta_col_fmt'] + "'')"
            print_info("TPT  Filter: {0}".format(l_extract_filter))
            l_extract_feed_filter =  l_test_condition + "CAST(''" + l_start_val + "'' AS " + arg_tbl_meta['delta_col_typ'] + \
                               " FORMAT ''" + arg_tbl_meta['delta_col_fmt'] + "'')" + \
                               " AND " + \
                               "CAST(''" + l_end_val + "'' AS " + arg_tbl_meta['delta_col_typ'] + \
                               " FORMAT ''" + arg_tbl_meta['delta_col_fmt'] + "'')"
            print_info("TPT  Filter: {0}".format(l_extract_feed_filter))
        elif arg_tbl_meta["db_type"] == 'ORACLE':
            l_extract_filter = arg_tbl_meta['delta_col'] + l_test_condition + \
                               "CAST('" + l_start_val + "' AS " + arg_tbl_meta['delta_col_typ'] + ")" + \
                               " AND " + \
                               "CAST('" + l_end_val + "' AS " + arg_tbl_meta['delta_col_typ'] + ")"
            print_info("SQLPlus Filter: {0}".format(l_extract_filter))
            l_extract_feed_filter =  l_test_condition + "CAST('" + l_start_val + "' AS " + arg_tbl_meta['delta_col_typ'] + ")" + \
                               " AND " + \
                               "CAST('" + l_end_val + "' AS " + arg_tbl_meta['delta_col_typ'] + ")"

    if "F" in run_switch:
        l_extract_filter = arg_tbl_meta["source_sql_txt"]
        l_extract_filter = l_extract_filter.replace("<<FROM_DT>>", str(l_start_val))
        l_extract_filter = l_extract_filter.replace("<<TO_DT>>", str(l_end_val))

    return l_extract_filter


def build_ptn_clause(arg_ptn_col_meta, arg_ins_sel_stmt):
    """
    :param arg_ptn_col_meta: A List/Array of columns in Source Table, plus a (sub) set of columns on the target
    :param arg_ins_sel_stmt: As partitioned columns are in addition to the regular set of columns we have to populate
                             the additional columns used for partitioing. Hence the INS..SEL statement used to populate
                             the table in REFINED DB is passed here and appeneded
    :rtype: 0 for success, -1 if an error occurs
    """
    l_partition_string = ""
    l_partition_ins_string = ""
    l_partition_cols_selected = ""
    ptn_dict = {}
    for i, v in enumerate(arg_ptn_col_meta):
        concat_char = " " if i == 0 else "\n, "
        if v[1] != "":
            hive_col_def = v[2] + "(" + v[3] + ")" if v[3] != "" else v[2]
            l_partition_string += concat_char + v[1] + "_PTN" + " " + hive_col_def
            l_partition_ins_string += concat_char + v[1] + "_PTN"

            # ------------------ Place holder for partition column imputation ---------------------
            if v[6] != '':
                ptn_col_formatted = v[6]
            else:
                ptn_col_formatted = v[1]

            l_partition_cols_selected += concat_char + "CAST(TRIM(" + ptn_col_formatted + ") AS " + \
                                         hive_col_def + ")" + " AS " + v[1] + "_PTN"

    if len(l_partition_cols_selected) > 1:
        arg_ins_sel_stmt = arg_ins_sel_stmt[:] + "\n," + l_partition_cols_selected
        # if len(l_partition_string) == 0:
        # l_partition_string = "LOADDATE STRING"
        # l_partition_ins_string = "LOADDATE"

    if len(l_partition_ins_string) != 0:
        l_partition_ins_string = "PARTITION (" + l_partition_ins_string + ")"
        l_partition_string = "PARTITIONED BY (" + l_partition_string + ")"

    ptn_dict["hive_rfnd_partition"] = l_partition_string
    ptn_dict["l_xtrnl_tbl_cols_selected"] = arg_ins_sel_stmt
    ptn_dict["hive_raw_partition"] = "PARTITIONED BY (LOADDATE STRING)"
    ptn_dict["delta_partition_clause"] = l_partition_ins_string

    return ptn_dict


def build_sqoop_query(arg_tbl_meta, src_col_list, arg_to_val, arg_from_val, arg_operator):
    """
    :
    :param arg_tbl_meta: A dictionary of Table level metadata indicating where it comes, what filters to use and
                         where to load
    :param src_col_list: A comma separated list of columns used for the Hive table in the "RAW" database
    List/Array of columns in Source Table, plus a (sub) set of columns on the target
    :param arg_from_val: Most likely a date value used to pull incremental data from source
    :param arg_to_val: Most likely a date value, used to end a range of dates for an incremental pull
    :param arg_operator: Possible values: >, >=, <, <=, =, <> used to build the condition
    :rtype: 0 for success, -1 if errored
    """

    # Sqoop - Query building
    # Strip the 1st comma from the src_col_list
    sqoop_query = ""
    # src_col_list = src_col_list[1:]
    if "SQOOP" in arg_tbl_meta["db_type"]:
        if arg_tbl_meta["rfrsh_rate"] == "SNAPSHOT":
            sqoop_query = '"' + "SELECT " + src_col_list + " FROM " + arg_tbl_meta["src_db"] + \
                          "." + arg_tbl_meta["src_tbl"] + " WHERE \\$CONDITIONS and 1=1" + '"'
        elif arg_tbl_meta["delta_col"] != "":
            if arg_to_val is not None:
                if arg_tbl_meta["db_type"] == 'SYBASE-SQOOP':
                        sqoop_query = '"' + "SELECT " + src_col_list + " FROM " + arg_tbl_meta["src_db"] \
                                    + "." + arg_tbl_meta["src_tbl"] + " WHERE \\$CONDITIONS and " + \
                                    "CONVERT(DATE, " + arg_tbl_meta["delta_col"] + ")" + " BETWEEN " + "CONVERT(DATE, '" + arg_from_val + "')" + " AND " +  "CONVERT(DATE, '" + arg_to_val  + "')" + '"'
                else:
                        sqoop_query = '"' + "SELECT " + src_col_list + " FROM " + arg_tbl_meta["src_db"] \
                              + "." + arg_tbl_meta["src_tbl"] + " WHERE \\$CONDITIONS and " + \
                              arg_tbl_meta["delta_col"] + " BETWEEN " + arg_from_val + " AND " + arg_to_val + '"'
            else:
                sqoop_query = '"' + "SELECT " + src_col_list + " FROM " + arg_tbl_meta["src_db"] \
                              + "." + arg_tbl_meta["src_tbl"] + " WHERE \\$CONDITIONS and " + \
                              arg_tbl_meta["delta_col"] + " " + arg_operator + " " + arg_from_val + '"'
        else:
                sqoop_query = '"' + "SELECT " + src_col_list + " FROM " + arg_tbl_meta["src_db"] \
                              + "." + arg_tbl_meta["src_tbl"] + " WHERE \\$CONDITIONS " + '"'

    return sqoop_query


def gen_script_from_tmplt(arg_base_dir, arg_tbl_meta, arg_col_meta, arg_dir_sep, arg_xtract_filter,
                          arg_from_val, arg_to_val, arg_operator, run_switch):
    """
    :param arg_base_dir: The location under which various scripts will be generated
    :param arg_tbl_meta: A dictionary of Table level metadata indicating where it comes, what filters to use and
                         where to load
    :param arg_col_meta: A List/Array of columns in Source Table, plus a (sub) set of columns on the target
    :param arg_dir_sep: Used for portability and testing. set to Backslash "\" in Windows else "/" for LINUX
    :param arg_xtract_filter: Used in the TPT or SQLPlus script generated
    :param arg_from_val: Passed on to the build_ptn_clause function
    :param arg_to_val: Passed on to the build_ptn_clause function
    :param arg_operator: Passed on to the build_ptn_clause function
    :param run_switch: Used as a switch to control building the Sqoop codegen
    :rtype: 0 for success, -1 if failed
    """

    tmplt_dir = arg_base_dir + 'config' + arg_dir_sep  # Dir where Metadata files & Templates for scripts are located
    l_extract_tmplt = ''
    l_extract_type = ''

    # voltage config directory where we keep configuration files under /main_proj/voltage/config
    voltage_config_dir = arg_base_dir + 'voltage' + arg_dir_sep + 'config' + arg_dir_sep + \
                         arg_tbl_meta["src_tbl"].lower() + arg_dir_sep

    # voltage lib directory where we keep library files under /main_proj/voltage/lib
    voltage_lib_dir = arg_base_dir + 'voltage' + arg_dir_sep + 'lib' + arg_dir_sep + \
                      arg_tbl_meta["src_tbl"].lower() + arg_dir_sep

    # voltage common libraries
    voltage_common_libs = arg_base_dir + 'voltage' + arg_dir_sep + 'libs' + arg_dir_sep

    passwd_file = arg_base_dir + 'common' + arg_dir_sep + 'ENV.scriptpwd.properties'

    script_names = {}  # Dictionary to hold paths to scripts generated
    script_names["codegen-run-switch"] = "N"  # Setting a default value for a switch, set to Y for Voltage only
    raw_tbl_tmplt = tmplt_dir + 'hive_raw_tbl.tmplt'
    hive_rfnd_tbl_tmplt = tmplt_dir + 'hive_rfnd_tbl.tmplt'
    ins_hive_rfnd_tmplt = tmplt_dir + 'ins_hive_rfnd_tbl.tmplt'
    alt_hive_raw_tmplt = tmplt_dir + 'alt_hive_raw_tbl.tmplt'
    vs_config_tmplt = tmplt_dir + 'vsconfig.tmplt'
#rl    tpt_feed_tmplt = tmplt_dir + 'tpt_feed.tmplt'

# ------------------- Check if templates exist else Abort  -----------------
    if not isfile(raw_tbl_tmplt) or not isfile(hive_rfnd_tbl_tmplt) or not isfile(alt_hive_raw_tmplt) or \
            not isfile(ins_hive_rfnd_tmplt):
        print_info("\n\tExpecting: " +
                   "\n\t\t\t " + raw_tbl_tmplt +
                   "\n\t\t\t " + hive_rfnd_tbl_tmplt +
                   "\n\t\t\t " + alt_hive_raw_tmplt +
                   "\n\t\t\t " + ins_hive_rfnd_tmplt)
        abort_with_msg("Templates for creating Hive Table not found")
    if arg_tbl_meta["db_type"] == 'TERADATA':
        if 'F' in run_switch:
          l_extract_tmplt = tmplt_dir + 'tpt_feed.tmplt'
        else:
          l_extract_tmplt = tmplt_dir + 'tpt.tmplt'
        l_extract_type = 'tpt'
        if not isfile(l_extract_tmplt):
            abort_with_msg("Templates for creating TPT scripts not found\n\t\tExpecting: " + l_extract_tmplt)
    elif arg_tbl_meta["db_type"] == 'ORACLE':
        if 'F' in run_switch:
          l_extract_tmplt = tmplt_dir + 'sqlplus_feed.tmplt'
        else:
          l_extract_tmplt = tmplt_dir + 'sqlplus.tmplt'
        l_extract_type = 'sql'
        if not isfile(l_extract_tmplt):
            abort_with_msg("Templates for creating sql scripts not found\n\t\tExpecting: " + l_extract_tmplt)
    elif "SQOOP" in arg_tbl_meta["db_type"]:
        l_extract_tmplt = tmplt_dir + 'sqoop.tmplt'
        l_extract_type = 'sqoop'
        if not isfile(l_extract_tmplt):
            abort_with_msg("Templates for creating Sqoop code not found\n\t\tExpecting: " + l_extract_tmplt)
    elif arg_tbl_meta["db_type"] == 'TERADATA-TDCH':
        l_extract_tmplt = tmplt_dir + 'tdch.tmplt'
        l_extract_type = 'tdch'
        if not isfile(l_extract_tmplt):
            abort_with_msg("Templates for creating sql scripts not found\n\t\tExpecting: " + l_extract_tmplt)

    # --------------------- Voltage config/lib/file check ----------------------
    # Place holder

    # ----------------- Generate the column lists from the array ------------------
    l_tpt_schma_cols = ""
    l_tpt_select_cols = ""
    l_raw_tbl_cols_for_ddl = ""
    l_refined_tbl_cols_for_ddl = ""
    l_raw_tbl_cols_selected = ""
    l_src_column_list = ""
    l_ptn_column_list = []
    l_pii_column_list = []
    l_ptn_col_only_list = []
    l_split_by_col = ""
    l_password = ""

    try:
        for i, v in enumerate(arg_col_meta):
            print_info(str(v))
            concat_char = " " if i == 0 else "\n, "
            src_concat_char = " " if i == 0 else ", "

            col_len = v[3] if v[3] != "" else "100"

            if l_split_by_col == "" and v[2] in ('INT', 'BIGINT', 'TINYINT'):
                l_split_by_col = v[0]
            ext_col_def = " VARCHAR(26)" if v[2] in ('DATE', 'TIMESTAMP', 'DECIMAL') else " VARCHAR(" + col_len + ")"
            raw_col_def = " STRING "
            l_tpt_schma_cols += concat_char + "VAR_" + str(i + 1) + ext_col_def
            l_tpt_select_cols += concat_char + "CAST(" + v[0] + " AS " + ext_col_def + ")"

            l_raw_tbl_cols_for_ddl += concat_char + v[1] + raw_col_def  # Col List for Hive raw Table

            if v[0] != "":
                l_src_column_list += src_concat_char + v[0]

            if v[1] != "":
                hive_col_def = v[2] + "(" + v[3] + ")" if v[3] != "" else v[2]
                l_refined_tbl_cols_for_ddl += concat_char + v[1] + " " + hive_col_def
            # ------------ Column has a specific format to use and the colum is not used for partitioning ------------

                if v[6] != '' and v[5] != "Y":
                    l_raw_tbl_cols_selected += concat_char + "CAST(TRIM(" + v[6] + ") AS " + hive_col_def + ")"
                else:
                    l_raw_tbl_cols_selected += concat_char + "CAST(TRIM(" + v[1] + ") AS " + hive_col_def + ")"

            # ----------- Build a subset list of columns used for partitioning in REFINED -----------
            if v[5] == "Y":
                if arg_tbl_meta["rfrsh_rate"] == "SNAPSHOT":
                    abort_with_msg("Table Metadata classifies table as SNAPSHOT. SNAPSHOT Tables are not Partitioned")
                else:
                    l_ptn_column_list.append(v)
                    l_ptn_col_only_list.append(v[1])

            # ----------- Build a subset list of PII columns -----------
            if v[5] == "Y":
                l_pii_column_list.append(v[0])
        # ------------ Call below to generate partition clauses for RAW and (conditionally) for REFINED --------------
        script_names["ptn_col_list"] = l_ptn_col_only_list
        ptn_clause_dict = build_ptn_clause(arg_ptn_col_meta=l_ptn_column_list,
                                           arg_ins_sel_stmt=l_raw_tbl_cols_selected)

        # ---------------- Add entries for location of partitions in the same dictionary -------------------
        ptn_clause_dict["raw_hdfs_location"] = arg_tbl_meta["hdfs_raw_dir"] + \
                                               arg_dir_sep + arg_tbl_meta["tgt_tbl"].lower() + "_raw"
        ptn_clause_dict["raw_hdfs_partition_location"] = '"' + ptn_clause_dict["raw_hdfs_location"] + \
                                                         arg_dir_sep + "LOADDATE=" + arg_from_val + '"'
        ptn_clause_dict["rfnd_hdfs_location"] = "'" + arg_tbl_meta["hdfs_refined_dir"] + arg_dir_sep + arg_tbl_meta[
            "tgt_tbl"].lower() + "'"
        ptn_clause_dict["ins_where_clause"] = "WHERE LOADDATE = '${hiveconf:inputsrcdt}'"
        ptn_clause_dict["alt_raw_partition"] = "(LOADDATE=" + arg_from_val + ")"
        # ----------------- Get DB User Password  ------------------
        if run_switch != "G":
            l_password = get_passwd(arg_user=arg_tbl_meta["db_user"].lower(), arg_passwd_file=passwd_file)
        # ----------------- Replace Placeholders in TPT Template and save a copy ------------------
        if l_extract_tmplt != "" and "F" not in run_switch:
            with open(l_extract_tmplt) as file_read_handle:
                contents = file_read_handle.read().replace("<<DB_NAME>>", arg_tbl_meta["src_db"])
                contents = contents.replace("<<TABLE_NAME>>", arg_tbl_meta["src_tbl"])
                contents = contents.replace("<<SCHEMA_LIST>>", l_tpt_schma_cols)
                contents = contents.replace("<<SEL_COLUMN_LIST>>", l_tpt_select_cols)
                contents = contents.replace("<<TDCH_COLUMN_LIST>>", l_src_column_list)  # Does not need CAST()
                contents = contents.replace("<<PREDICATE>>", arg_xtract_filter)
                contents = contents.replace("<<DELIMITER>>", arg_tbl_meta["stg_tbl_delimiter"])
                contents = contents.replace("<<USERID>>", arg_tbl_meta["db_user"])
                contents = contents.replace("<<USER_PASSWD>>", l_password)
                contents = contents.replace("<<HDFS_EXTRACT_PTN_LOC>>", ptn_clause_dict["raw_hdfs_partition_location"])

            # The xtract_dir is set to $HOME/tmp/tpt (or sql) to keep dynamically generated files out of GIT control
            l_xtract_dir = os.path.expanduser('~') + arg_dir_sep + "staging" + arg_dir_sep + \
                           l_extract_type + arg_dir_sep
            script_names["extract"] = l_xtract_dir + arg_tbl_meta["src_db"].lower() + "." \
                                      + arg_tbl_meta["src_tbl"].lower() + "." + l_extract_type

            write_content(script_names["extract"], contents, "Y")
        print ("debug 1")

   # ----------------- Replace Placeholders in FEED TPT Template and save a copy ------------------
        if l_extract_tmplt != "" and "F" in run_switch:
            with open(l_extract_tmplt) as file_read_handle:
#               l_xtract_filter = "WHERE " + arg_xtract_filter
                contents = file_read_handle.read()
                contents = contents.replace("<<TABLE_NAME>>", arg_tbl_meta["src_tbl"])
                contents = contents.replace("<<SCHEMA_LIST>>", l_tpt_schma_cols)
                contents = contents.replace("<<SEL_COLUMN_LIST>>", l_tpt_select_cols)
                contents = contents.replace("<<PREDICATE>>", arg_xtract_filter)
                contents = contents.replace("<<DELIMITER>>", arg_tbl_meta["stg_tbl_delimiter"])
                contents = contents.replace("<<USERID>>", arg_tbl_meta["db_user"])
                contents = contents.replace("<<USER_PASSWD>>", l_password)
                contents = contents.replace("<<HDFS_EXTRACT_PTN_LOC>>", ptn_clause_dict["raw_hdfs_partition_location"])

            # The xtract_dir is set to $HOME/tmp/tpt (or sql) to keep dynamically generated files out of GIT control
            l_xtract_dir = os.path.expanduser('~') + arg_dir_sep + "staging" + arg_dir_sep + \
                           l_extract_type + arg_dir_sep
            script_names["extract"] = l_xtract_dir + arg_tbl_meta["src_db"].lower() + "." \
                                      + arg_tbl_meta["src_tbl"].lower() + "." + l_extract_type
            write_content(script_names["extract"], contents, "Y")



        # ------------- Replace Placeholders in RAW Table and save a copy --------------
        with open(raw_tbl_tmplt) as file_read_handle:
            contents = file_read_handle.read().replace("<<HIVE_RAW_DB_NM>>", arg_tbl_meta["stg_db"])
            contents = contents.replace("<<HIVE_RAW_TBL_NM>>", arg_tbl_meta["tgt_tbl"] + "_RAW")
            contents = contents.replace("<<HIVE_RAW_TBL_COL_LIST>>", l_raw_tbl_cols_for_ddl)
            contents = contents.replace("<<DELIMITER>>", arg_tbl_meta["stg_tbl_delimiter"])
            contents = contents.replace("<<PARTITION_CLAUSE>>", ptn_clause_dict["hive_raw_partition"])
            contents = contents.replace("<<HDFS_RAW_LOCATION>>", "'" + ptn_clause_dict["raw_hdfs_location"] + "'")

        # The deploy_dir is set to $HOME/tmp/tpt (or sql) to keep dynamically generated files out of GIT control
        l_deploy_dir = os.path.expanduser('~') + arg_dir_sep + "staging" + arg_dir_sep + "deploy" + arg_dir_sep

        script_names["hive_ext_tbl"] = l_deploy_dir + arg_tbl_meta["stg_db"].lower() + "." + \
                                       arg_tbl_meta["tgt_tbl"].lower() + "_raw" + ".hql"
        write_content(script_names["hive_ext_tbl"], contents, "Y")

        # -------------- Replace Placeholders in REFINED Table and save a copy ---------------
        tgt_tbl_bkt_clause = "CLUSTERED BY " + \
                             arg_tbl_meta["tgt_tbl_bkt_clause"] if arg_tbl_meta["tgt_tbl_bkt_clause"] != "" else ""
        # tgt_tbl_ptn_clause = "PARTITIONED BY (" + arg_tbl_meta["tgt_tbl_ptn_clause"] + ")" \
        #                     if arg_tbl_meta["tgt_tbl_ptn_clause"] != "" else ""
        with open(hive_rfnd_tbl_tmplt) as file_read_handle:
            contents = file_read_handle.read().replace("<<HIVE_RFND_DB_NM>>", arg_tbl_meta["tgt_db"])
            contents = contents.replace("<<HIVE_RFND_TBL_NM>>", arg_tbl_meta["tgt_tbl"])
            contents = contents.replace("<<HIVE_RFND_TBL_COL_LIST>>", l_refined_tbl_cols_for_ddl)
            if arg_tbl_meta["rfrsh_rate"] == "SNAPSHOT":
                contents = contents.replace("<<PARTITION_CLAUSE>>", "")
            else:
                contents = contents.replace("<<PARTITION_CLAUSE>>", ptn_clause_dict["hive_rfnd_partition"])
            contents = contents.replace("<<BUCKET_CLAUSE>>", tgt_tbl_bkt_clause)
            contents = contents.replace("<<HDFS_RFND_LOCATION>>", ptn_clause_dict["rfnd_hdfs_location"])

        script_names["hive_rfnd_tbl"] = l_deploy_dir + arg_tbl_meta["tgt_db"].lower() + "." + \
                                        arg_tbl_meta["tgt_tbl"].lower() + ".hql"
        write_content(script_names["hive_rfnd_tbl"], contents, arg_tbl_meta["refresh_refined_DDL_DML"])

        # -------------- Replace Placeholders in INS..SEL template and save a copy ---------------
        with open(ins_hive_rfnd_tmplt) as file_read_handle:
            contents = file_read_handle.read().replace("<<HIVE_RFND_DB_NM>>", arg_tbl_meta["tgt_db"])
            contents = contents.replace("<<HIVE_RFND_TBL_NM>>", arg_tbl_meta["tgt_tbl"])
            contents = contents.replace("<<HIVE_RAW_TBL_COL_LIST>>", ptn_clause_dict["l_xtrnl_tbl_cols_selected"])
            contents = contents.replace("<<HIVE_RAW_DB_NM>>", arg_tbl_meta["stg_db"])
            contents = contents.replace("<<HIVE_RAW_TBL_NM>>", arg_tbl_meta["tgt_tbl"] + "_RAW")
            contents = contents.replace("<<WHERE_CLAUSE>>", ptn_clause_dict["ins_where_clause"])
            if arg_tbl_meta["rfrsh_rate"] == "SNAPSHOT":
                contents = contents.replace("<<PARTITION_CLAUSE>>", "")
            else:
                contents = contents.replace("<<PARTITION_CLAUSE>>", ptn_clause_dict["delta_partition_clause"])

        script_names["ins_hive_rfnd_tbl"] = l_deploy_dir + \
                                            arg_tbl_meta["tgt_tbl"].lower() + ".hql"
        write_content(script_names["ins_hive_rfnd_tbl"], contents, arg_tbl_meta["refresh_refined_DDL_DML"])

        with open(alt_hive_raw_tmplt) as file_read_handle:
            contents = file_read_handle.read().replace("<<HIVE_RAW_DB_NM>>", arg_tbl_meta["stg_db"])
            contents = contents.replace("<<HIVE_RAW_TBL_NM>>", arg_tbl_meta["tgt_tbl"] + "_RAW")
            contents = contents.replace("<<HIVE_RAW_PARTITION>>", ptn_clause_dict["alt_raw_partition"])
            contents = contents.replace("<<HIVE_RAW_PARTITION_LOCATION>>",
                                        ptn_clause_dict["raw_hdfs_partition_location"])
        script_names["alt_raw_tbl"] = l_deploy_dir + \
                                      arg_tbl_meta["tgt_tbl"].lower() + "_alt" + ".hql"
        write_content(script_names["alt_raw_tbl"], contents, "Y")

        # ----------------------------  Build Sqoop Query portion ----------------------------
        if arg_tbl_meta["split_by"] is not None and arg_tbl_meta["split_by"].strip() != "":
                ptn_clause_dict["split_by_col"] = "--split-by " + arg_tbl_meta["split_by"]
                ptn_clause_dict["num_mappers"] = "--num-mappers 10"
        elif l_split_by_col != "" and l_split_by_col is not None:
                ptn_clause_dict["split_by_col"] = "--split-by " + l_split_by_col
                ptn_clause_dict["num_mappers"] = "--num-mappers 10"
        else:
                ptn_clause_dict["split_by_col"] = ""
                ptn_clause_dict["num_mappers"] = "--num-mappers 1"
        ptn_clause_dict["sqoop_oracle_query"] = build_sqoop_query(arg_tbl_meta=arg_tbl_meta,
                                                                  src_col_list=l_src_column_list,
                                                                  arg_to_val=arg_to_val,
                                                                  arg_from_val=arg_from_val,
                                                                  arg_operator=arg_operator)

#        print("arg_tbl_meta=" + str(arg_tbl_meta) + "\n" +  "src_col_list=" + str(l_src_column_list) + "\n" + "arg_to_val=" + str(arg_to_val) + "\n" +  "arg_from_val=" + str(arg_from_val) + "\n" + "arg_operator="+ str(arg_operator))


        # -------------------------------- Build Sqoop command from template ------------------
        if "SQOOP" in arg_tbl_meta["db_type"]:
            l_sqoop_dir = os.path.expanduser('~') + arg_dir_sep + "staging" + arg_dir_sep + 'sqoop' + arg_dir_sep

            with open(l_extract_tmplt) as file_read_handle:
                contents = file_read_handle.read().replace("<<CONNECTION_STRING>>", arg_tbl_meta["db_server"])
                contents = contents.replace("<<DB_USER_NAME>>", arg_tbl_meta["db_user"])
                contents = contents.replace("<<DB_PASSWORD>>", l_password)  # update passwd
                contents = contents.replace("<<SQOOP_QUERY>>", ptn_clause_dict["sqoop_oracle_query"])
                contents = contents.replace("<<WANT_COMPRESSION>>", "--compress")
                contents = contents.replace("<<COMPRESSION_CODEC>>", "--compression-codec snappy")
                contents = contents.replace("<<HDFS_RAW_LOCATION>>", ptn_clause_dict["raw_hdfs_partition_location"])
                contents = contents.replace("<<SPLIT_BY_COL>>", ptn_clause_dict["split_by_col"])
                contents = contents.replace("<<NUM_MAPPERS>>", ptn_clause_dict["num_mappers"])
                contents = contents.replace("<<DELIMITER>>", arg_tbl_meta["stg_tbl_delimiter"])


                if "ORACLE" in arg_tbl_meta["db_type"]:
                    contents = contents.replace("<<DB_DRIVER>>","")
                    contents = contents.replace("<<DB_CONNECTION_PREFIX>>","jdbc:oracle:thin:@")
                elif "SYBASE" in arg_tbl_meta["db_type"]:
                    contents = contents.replace("<<DB_DRIVER>>","--driver com.sybase.jdbc4.jdbc.SybDriver")
                    contents = contents.replace("<<DB_CONNECTION_PREFIX>>","jdbc:sybase:Tds:")

                if arg_tbl_meta["db_type"] == 'ORACLE-SQOOP-TNS':
                    contents = contents.replace("<<MAPRED_JAVA_OPTS>>",
                                                "-D mapred.map.child.java.opts='-Doracle.net.tns_admin=.'")
                    contents = contents.replace("<<FILES_OPTION>>",
                                                "-files $TNS_ADMIN/tnsnames.ora,$TNS_ADMIN/sqlnet.ora")
                    contents = contents.replace("<<LIBJARS_VOLTAGE>>", "").replace("<<VOLTAGE_JAR_FILE>>", "")
                    contents = contents.replace("<<VOLTAGE_CLASS_NAME>>", "")
                elif arg_tbl_meta["db_type"] == 'ORACLE-SQOOP-TNS-VOLTAGE' or \
                                arg_tbl_meta["db_type"] == 'ORACLE-SQOOP-VOLTAGE':

                    l_bin_dir = l_sqoop_dir + arg_tbl_meta["src_tbl"] + arg_dir_sep
                    # -------------------- Codegen code generation -------------------------
                    script_names["sqoop-codegen"] = "sqoop codegen --username " + arg_tbl_meta["db_user"] + \
                                                    " --password " + l_password + " --connect jdbc:oracle:thin:@" + \
                                                    arg_tbl_meta["db_server"] + \
                                                    "  --query " + ptn_clause_dict["sqoop_oracle_query"] + \
                                                    " --class-name com.voltage.sqoop.DataRecord " + \
                                                    " --fields-terminated-by '\\001' " + \
                                                    " --null-non-string '\\\N' " + " --null-string '\\\N' " + \
                                                    " --bindir " + l_bin_dir + \
                                                    " --outdir " + l_bin_dir
                    full_codegen_path = l_sqoop_dir + arg_tbl_meta["src_tbl"].lower() + ".codegen." + 'sqoop'

                    if "E" in run_switch:
                        dir_check_create(full_file_path=full_codegen_path)
                        write_content(full_codegen_path, script_names["sqoop-codegen"], "Y")
                    # If codegen jar not found at default location, use the /tmp/sqoop path for codegen jar

                    if not os.path.isfile(voltage_lib_dir + "com.voltage.sqoop.DataRecord.jar") and run_switch != "G":
                        dir_check_create(full_file_path=l_bin_dir + "com.voltage.sqoop.DataRecord.jar")
                        voltage_lib_dir = l_bin_dir
                        script_names["codegen-run-switch"] = "Y"
                        print_info("Codegen used from path " + voltage_lib_dir)
                    if "E" in run_switch:
                        # -------------  Build vsconfig.properties and move it to HDFS -------------------
                        vsconfig_hdfs_path = "".join(open(voltage_config_dir +
                                                          "config-locator.properties").readlines()[0].
                                                     rstrip('\n').split("=")[1])
                        f = open(vs_config_tmplt).read()
                        l_tbl_prop = f.split('# Section for Columns')[0]
                        l_col_prop = f.split('# Section for Columns')[1]

                        l_tbl_prop = l_tbl_prop.replace("<<TABLE_NAME>>", arg_tbl_meta["src_tbl"])
                        write_content(l_deploy_dir + 'vsconfig.properties', l_tbl_prop, "Y")

                        l_col_prop = l_col_prop.replace("<<USERID>>", arg_tbl_meta["src_tbl"])
                        l_col_prop = l_col_prop.replace("<<USER_PASSWD>>", arg_tbl_meta["src_tbl"])
                        for idx, val in enumerate(l_pii_column_list):
                            l_col_prop_to_write = l_col_prop.replace("<<PII_COL_NAME>>", val)
                            l_col_prop_to_write = l_col_prop_to_write.replace("<<CTR>>", str(idx))
                            # We override metadata value of Y/N to support Appending to a file generated from template
                            write_content(l_deploy_dir + 'vsconfig.properties', l_col_prop_to_write, 'A')

                        copy_to_hdfs(arg_input_path=l_deploy_dir + 'vsconfig.properties',
                                     arg_tgt_hdfs_config_file=vsconfig_hdfs_path)
                        run_shell_cmd("rm " + l_deploy_dir + 'vsconfig.properties')

                    if arg_tbl_meta["db_type"] == 'ORACLE-SQOOP-TNS-VOLTAGE':
                        contents = contents.replace("<<MAPRED_JAVA_OPTS>>",
                                                    "-D mapred.map.child.java.opts='-Doracle.net.tns_admin=.'")
                        contents = contents.replace("<<FILES_OPTION>>",
                                                    "-files $TNS_ADMIN/tnsnames.ora," +
                                                    "$TNS_ADMIN/sqlnet.ora," +
                                                    voltage_config_dir + "config-locator.properties," +
                                                    voltage_common_libs + "libvibesimplejava.so")
                    else:
                        contents = contents.replace("<<MAPRED_JAVA_OPTS>>", "")
                        contents = contents.replace("<<FILES_OPTION>>", "-files " +
                                                    voltage_config_dir + "config-locator.properties," +
                                                    voltage_common_libs + "libvibesimplejava.so")
                        contents = contents.replace("<<LIBJARS_VOLTAGE>>", "-libjars " +
                                                    voltage_lib_dir + "com.voltage.sqoop.DataRecord.jar," +
                                                    voltage_common_libs + "Hadoop-Voltage-jar-with-dependencies.jar," +
                                                    voltage_common_libs + "vibesimplejava.jar")
                        contents = contents.replace("<<VOLTAGE_JAR_FILE>>", "--jar-file " +
                                                    voltage_common_libs + "Hadoop-Voltage.jar")
                        contents = contents.replace("<<VOLTAGE_CLASS_NAME>>",
                                                    "--class-name com.voltage.securedata.hadoop.sqoop.SqoopImportProtector")
                elif arg_tbl_meta["db_type"] == 'ORACLE-SQOOP' or arg_tbl_meta["db_type"] == 'SYBASE-SQOOP':
                    contents = contents.replace("<<MAPRED_JAVA_OPTS>>", "").replace("<<FILES_OPTION>>", ""). \
                        replace("<<LIBJARS_VOLTAGE>>", "").replace("<<VOLTAGE_JAR_FILE>>", ""). \
                        replace("<<VOLTAGE_CLASS_NAME>>", "")

            script_names["sqoop-extract"] = contents
            # The xtract_dir is set to $HOME/tmp/tpt (or sql or sqoop) to keep dynamically generated files out of GIT
            # l_sqoop_dir = os.path.expanduser('~') + arg_dir_sep + "staging" + arg_dir_sep + 'sqoop' + arg_dir_sep

            script_names["extract"] = l_sqoop_dir + arg_tbl_meta["src_db"].lower() + "." \
                                      + arg_tbl_meta["src_tbl"].lower() + "." + 'sqoop'

            write_content(script_names["extract"], contents, "Y")

        if len(script_names) == 0:
            abort_with_msg("No Templates Generated")

        # ----------------------------  Merging two dictionaries ----------------------------
        helper_dict = script_names.copy()
        helper_dict.update(ptn_clause_dict)
        return helper_dict

    except Exception as err:
        abort_with_msg("An exception of type " + type(err).__name__ + "occured. Arguments:\n" + str(err.args))


def chk_if_exists_in_hdfs(arg_hdfs_path, arg_type):
    if arg_type == 'F':
        test_hdfs_path = "hadoop fs -test -e " + arg_hdfs_path  # Test for File
    elif arg_type == 'D':
        test_hdfs_path = "hadoop fs -test -d " + arg_hdfs_path  # Test for Directory

    return run_shell_cmd(arg_shell_cmd=test_hdfs_path)


def del_in_hdfs(arg_hdfs_path):
    del_hdfs_path = "hadoop fs -rm -R -skipTrash " + arg_hdfs_path
    status = run_shell_cmd(arg_shell_cmd=del_hdfs_path)
    print_info("File Deleted: " + arg_hdfs_path) if status == 0 else abort_with_msg("HDFS File not deleted")


def copy_to_hdfs(arg_input_path, arg_tgt_hdfs_config_file):
    output_hdfs_dir = os.path.dirname(arg_tgt_hdfs_config_file)
    if chk_if_exists_in_hdfs(arg_hdfs_path=output_hdfs_dir, arg_type='D') != 0:  # Directory Does Not Exists
        if run_shell_cmd("hadoop fs -mkdir -p " + output_hdfs_dir) == 0:
            print_info("Created Dir: " + output_hdfs_dir)
        else:
            abort_with_msg("Dir Creation Failed")

    if run_shell_cmd("hadoop fs -put " + arg_input_path + " " + output_hdfs_dir) == 0:
        print_info("File Copied to: " + arg_tgt_hdfs_config_file)
    else:
        abort_with_msg("Copy Failed")


def decompress(arg_compress_file, arg_output_dir, arg_pig_script):
    decompress_cmd = "pig -Dmapred.job.queue.name=$HIVEQUEUE" + \
                     " -param INPUTFILE=" + arg_compress_file + \
                     " -param OUTPUTDIR=" + arg_output_dir + \
                     " " + arg_pig_script
    run_shell_cmd(decompress_cmd)
    return


def run_extract(arg_tbl_meta, arg_log_dir, arg_passwd_file, arg_date_for_extract, arg_helper_dict):
    l_dir_path, l_script_name = os.path.split(arg_helper_dict["extract"])
    l_cmd_extract = ''
    # GlobalValues.extract_file = ''

    # -------------------- Clean up raw HDFS file first ----------------------
    if chk_if_exists_in_hdfs(arg_hdfs_path=arg_helper_dict["raw_hdfs_partition_location"], arg_type='D') == 0:
        del_in_hdfs(arg_hdfs_path=arg_helper_dict["raw_hdfs_partition_location"])

    if arg_tbl_meta["db_type"] == 'ORACLE':
        l_cmd_mkdir_hdfs = "hadoop fs -mkdir " + arg_helper_dict["raw_hdfs_partition_location"]
        run_shell_cmd(arg_shell_cmd=l_cmd_mkdir_hdfs)

    print_info("Extracting data from " + arg_tbl_meta["db_type"] + " using " + arg_helper_dict["extract"])
    l_password = get_passwd(arg_user=arg_tbl_meta["db_user"].lower(),
                            arg_passwd_file=arg_passwd_file)
    if arg_tbl_meta["db_type"] == 'TERADATA':
        GlobalValues.extract_log = arg_log_dir + "tpt." + \
                                   arg_tbl_meta["src_db"].lower() + "." + arg_tbl_meta["src_tbl"].lower() + ".log"
        GlobalValues.extract_file = arg_tbl_meta["src_db"].lower() + "." + \
                            arg_tbl_meta["src_tbl"].lower() + "." + \
                            arg_date_for_extract.replace("/", "") + ".txt.gz"

        # Used single quotes to enclose double quotes and vice versa
        l_cmd_extract = ' tbuild -f ' + arg_helper_dict["extract"] + \
                        ' -L ' + arg_log_dir + \
                        ' -j "' + l_script_name + '" -u "' + \
                        "UserID='" + arg_tbl_meta["db_user"] + \
                        "', UserPwd='" + l_password + \
                        "', DirPath='" + arg_tbl_meta["extract_landing_dir"] + \
                        "', OutFileName='" + GlobalValues.extract_file + \
                        "', ServerNm='" + arg_tbl_meta["db_server"] + "' " + '"  >  ' + GlobalValues.extract_log

    if arg_tbl_meta["db_type"] == 'TERADATA-TDCH':
        GlobalValues.extract_log = arg_log_dir + "tpt." + \
                                   arg_tbl_meta["src_db"].lower() + "." + arg_tbl_meta["src_tbl"].lower() + ".log"
        # Used single quotes to enclose double quotes and vice versa
        l_cmd_extract = "sh " + arg_helper_dict["extract"] + " > " + GlobalValues.extract_log

    elif arg_tbl_meta["db_type"] == 'ORACLE':
        GlobalValues.extract_file = arg_tbl_meta["src_db"].lower() + "." + \
                            arg_tbl_meta["src_tbl"].lower() + "." + \
                            arg_date_for_extract.replace("/", "") + ".txt"
        l_cmd_extract = "sqlplus -S " + arg_tbl_meta["db_user"] + "/" + l_password + \
                        "@" + arg_tbl_meta["db_server"] + \
                        " @" + arg_helper_dict["extract"] + " > " + \
                        arg_tbl_meta["extract_landing_dir"] + GlobalValues.extract_file

    elif "SQOOP" in arg_tbl_meta["db_type"]:
        if arg_helper_dict["codegen-run-switch"] == "Y":
            l_return_code = run_shell_cmd(arg_shell_cmd=arg_helper_dict["sqoop-codegen"])
            print_info("Codegen created successfully") if l_return_code == 0 else abort_with_msg("Codegen Failed")

        l_cmd_extract = arg_helper_dict["sqoop-extract"]

    # -------------- Run extraction only if the extract is NOT present, and directly movefile to HDFS -----------------
    l_dir_sep = "/" if arg_tbl_meta["extract_landing_dir"][-1:] != "/" else ""

    if ((arg_tbl_meta["db_type"] == 'TERADATA' or arg_tbl_meta["db_type"] == 'ORACLE') and
            isfile(arg_tbl_meta["extract_landing_dir"] + l_dir_sep + GlobalValues.extract_file)):
        print_warn(arg_tbl_meta["extract_landing_dir"] + l_dir_sep + GlobalValues.extract_file +
                   " Exists. !!Skipping Extract!!")
    else:
    # -------------------- Running the extract and handling return code ----------------------
        l_return_code = run_shell_cmd(arg_shell_cmd=l_cmd_extract)
        print_info("The return code is " + str(l_return_code))
        if l_return_code == 0:
            print_info("Shell Execution Completed Successfully.")
        else:
            if arg_tbl_meta["db_type"] == 'TERADATA' and (l_return_code == 1024 or l_return_code == 4):
                print_warn("Extraction of data completed with Warnings. Please Check Log")
            else:
                abort_with_msg("Shell Execution Failed: "+ str(l_return_code))

    # ------------------------------------------ Fetch Counts ------------------------------------
    if arg_tbl_meta["db_type"] == 'TERADATA':
        error_msg = getstatusoutput('grep -i error ' + GlobalValues.extract_log)[1]
        if error_msg != '':
            abort_with_msg(error_msg)
        else:
            GlobalValues.extract_count = getstatusoutput('grep -i Exported ' +
                                                         GlobalValues.extract_log + '|cut -d: -f3')[1].lstrip()
            print("Teradata count: "+str(GlobalValues.extract_count))
    elif arg_tbl_meta["db_type"] == 'ORACLE':
        GlobalValues.extract_count = sum(1 for line in open(arg_tbl_meta["extract_landing_dir"] + \
                                               l_dir_sep + GlobalValues.extract_file))
        print("Oracle count: "+str(GlobalValues.extract_count))
    # -------------- V1 framework takes the extract and uses Shell scripts for further processing. Hence EXIT.-----------------
    if arg_tbl_meta["v1_support"] == "Y":
        exit(0)

    # -------------- Move file to HDFS. Needed only for TPT and SQL Plus output-----------------
    if arg_tbl_meta["db_type"] == 'ORACLE':
        l_cmd_move_to_hdfs = "hadoop fs -put " + \
                             arg_tbl_meta["extract_landing_dir"] + GlobalValues.extract_file + " " + \
                             arg_helper_dict["raw_hdfs_partition_location"]
        run_shell_cmd(arg_shell_cmd=l_cmd_move_to_hdfs)
    elif arg_tbl_meta["db_type"] == 'TERADATA':
        if chk_if_exists_in_hdfs(arg_tbl_meta["hdfs_basedir"] + "/" +
                                         arg_tbl_meta["hdfs_extract_dir"] + "/" +
                                         GlobalValues.extract_file,
                                 arg_type='F') == 0:  # File Exists
            del_in_hdfs(arg_tbl_meta["hdfs_basedir"] + "/" +
                        arg_tbl_meta["hdfs_extract_dir"] + "/" +
                        GlobalValues.extract_file)

        l_cmd_move_to_hdfs = "hadoop fs -put " + \
                             arg_tbl_meta["extract_landing_dir"] + GlobalValues.extract_file + " " + \
                             arg_tbl_meta["hdfs_basedir"] + "/" + arg_tbl_meta["hdfs_extract_dir"]

        run_shell_cmd(arg_shell_cmd=l_cmd_move_to_hdfs)

        decompress(arg_compress_file=arg_tbl_meta["hdfs_basedir"] + "/" +
                                     arg_tbl_meta["hdfs_extract_dir"] + "/" +
                                     GlobalValues.extract_file,
                   arg_output_dir=arg_helper_dict["raw_hdfs_partition_location"],
                   arg_pig_script="$SCRDIR/uncompress_hdfsfiles.pig")
    return arg_tbl_meta["extract_landing_dir"] + l_dir_sep + GlobalValues.extract_file, GlobalValues.extract_count


def run_shell_cmd(arg_shell_cmd):
    import subprocess
    try:
        print_info(MsgColors.OKGREEN + "Running : " + arg_shell_cmd + MsgColors.ENDC)
        p = subprocess.Popen(arg_shell_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        (output, error) = p.communicate()
        rc = p.wait()   # Investigate behavior
        if rc != 0 and error is not None:
            print_info("Output from Shell Execution " + output)
            abort_with_msg("Shell execution failed with return_code " + str(rc) + " and error " + str(error))
        else:
            print_info(output)
        return rc
    except Exception as err:
        abort_with_msg("An exception of type " + type(err).__name__ + "occured. Arguments:\n" + str(err.args))


def run_hql(arg_script, arg_mode='f', arg_param=''):
    """
    :param arg_script: The full path to the .HQL file
    :arg_mode - Optional takes 'e' or 'f'. Default is file mode

    :rtype: 0 for success, -1 if errored
    """

    if arg_mode == 'e':
        l_shell_cmd = 'beeline -u $HIVEHOST -e ' + arg_script + ' --hiveconf mapred.job.queue.name=$HIVEQUEUE'
    else:
        l_shell_cmd = 'beeline -u $HIVEHOST -f ' + arg_script + ' --hiveconf mapred.job.queue.name=$HIVEQUEUE' + \
                      arg_param
    try:
        if run_shell_cmd(l_shell_cmd) == 0:
            print_info("SQL completed successfully.")
        else:
            abort_with_msg("HQL Execution failed")
    except Exception as err:
        abort_with_msg("An exception of type " + type(err).__name__ + "occured. Arguments:\n" + str(err.args))


def run_dq(arg_tbl_meta, arg_log_dir, from_date, end_date, part_col, which_table):
    l_shell_cmd = 'spark-submit ' + \
                  '--queue root.opsdata.batch ' + \
                  '--master yarn ' + \
                  '--deploy-mode client ' + \
                  '--num-executors 5 $SCRDIR/datavalidation.py ' + \
                  ' -d ' + arg_tbl_meta["tgt_db"] + \
                  ' -t ' + arg_tbl_meta["tgt_tbl"]

    if from_date is not None:
        l_shell_cmd = l_shell_cmd + ' -f ' + from_date
    if end_date is not None:
        l_shell_cmd = l_shell_cmd + ' -e ' + end_date
    if part_col is not None:
        l_shell_cmd = l_shell_cmd + ' -p ' + part_col
    if which_table is not None:
        l_shell_cmd = l_shell_cmd + ' -w ' + which_table

    try:
        print_info("Running DQ: " + l_shell_cmd)
        rc1 = run_shell_cmd(l_shell_cmd)
        print("return code from data quality check is " + str(rc1))
        if rc1 == 0:
            print_info("DQ completed successfully. ")
        else:
            abort_with_msg("DQ Check Failed")
    except Exception as err:
        abort_with_msg("An exception of type " + type(err).__name__ + "occured. Arguments:\n" + str(err.args))


def check_hdfs_space(arg_hdfs_base_path,part_col=None,from_dt=None,end_dt=None):
    from datetime import datetime
#   import datetime
    import time
    import subprocess
    from decimal import Decimal

    if chk_if_exists_in_hdfs(arg_hdfs_base_path,'D') != 0:
        abort_with_msg("HDFS directory does not exist :" + arg_hdfs_base_path)

    if end_dt is not None and from_dt is not None and part_col is not None:
        from_datestring = datetime.strptime(from_dt,'%Y%m%d').strftime('%Y%m%d')
        to_datestring = datetime.strptime(end_dt,'%Y%m%d').strftime('%Y%m%d')
        shellcmd = 'hdfs dfs -ls ' + arg_hdfs_base_path + ' |tail -n +2 | cut -d\'/\' -f7'
        p= subprocess.Popen([shellcmd], stdout=subprocess.PIPE, shell=True)
        dList=p.stdout.read()
        dList=dList.split('\n')
        raw_size = 0.0
        refined_size  = 0.0
        for rec in dList:
            if rec == ' ' or rec =='':
                continue
            dirdate=(rec.split('=')[-1])[:8]
            if time.strptime(dirdate,'%Y%m%d') >= time.strptime(from_datestring,'%Y%m%d') and \
               time.strptime(dirdate,'%Y%m%d') <= time.strptime(to_datestring,'%Y%m%d'):
               cmd = "hadoop fs -du -h -s " + arg_hdfs_base_path + "/" + part_col + "=" + dirdate
               proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
               (out, err) = proc.communicate()
               if proc.returncode != 0:
                  abort_with_msg("HDFS space check failed : " + cmd + " With return code  " + str(proc.returncode))
               item = [item for item in out.split(" ") if item]
               if len(item) == 5:
                    raw_size = Decimal(convert_GB(item[0],item[1])) + Decimal(raw_size)
                    refined_size = Decimal(convert_GB(item[2],item[3])) + Decimal(refined_size)
               else:
                    abort_with_msg("HDFS -du -h -s check failed")
        return (format(raw_size,'.2f')+"G",format(refined_size,'.2f')+"G")
    if part_col is None and from_dt is None and end_dt is None:
        arg_hdfs_path = arg_hdfs_base_path
    elif part_col is not None and from_dt is not None and end_dt is None:
        arg_hdfs_path = arg_hdfs_base_path + "/" + part_col + "=" + from_dt

    cmd = "hadoop fs -du -h -s " + arg_hdfs_path
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    (out, err) = proc.communicate()
    if proc.returncode != 0:
        abort_with_msg("HDFS space check failed : " + cmd + " With return code  " + str(proc.returncode))

    item = [item for item in out.split(" ") if item]
    if len(item) == 5:
       return (item[0]+item[1],item[2]+item[3])
    else:
       print_warn("hadoop space command giving more output than anticipated : " + str(item))
       return ("0G","0G")


def convert_GB(size,unit):
    from decimal import Decimal
    if unit == 'P':
       return format(Decimal(size)*1000000,'.2f')
    elif unit == "T":
       return format(Decimal(size)*1000,'.2f')
    elif unit == "G":
       return str(size)
    elif unit == "M":
       return format(Decimal(size)*Decimal(0.001),'.2f')
    elif unit == "K":
       return format(Decimal(size)*Decimal(0.000001),'.2f')
    else:
       return str(size)
