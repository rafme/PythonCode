#!/usr/bin/python
from platform import system
from datetime import datetime
from timeit import default_timer as timer
#from utilities import mySQL_helper
from time import time
from mySQLMetadata import mySQLMetadata
from mySQLHandler import mySQLHandler
import os
import optparse
import utilities
import logging
import purgepartitions

class LocalValues:
    db_nm = ''
    tbl_nm = ''
    date_to_process = ''
    job_pid = ''
    process_start = datetime.now()
    log_filename = ''
    mysql_prop_file = ''
    rowcount = 0
    audit_tbl = 'ops_jobstat'       # Hard coded value, as we do not keep the table name in a common ENV file.
    raw_size = ''
    raw_replica_size = ''
    refined_size = ''
    refined_replica_size = ''

def set_audit_action():
    if LocalValues.job_pid == 0:
        LocalValues.job_pid = int(round(time() * 1000))
        LocalValues.process_start = datetime.now()
        return 'insert'
    else:
        return 'update'


def audit_log(audit_action, arg_status_msg, arg_count=0, arg_extract_nm=''):
    l_process_end = datetime.now()
    utilities.GlobalValues.record = {'feed': LocalValues.tbl_nm,
              'jobpid': LocalValues.job_pid,
              'filename': arg_extract_nm,
              'process_start': LocalValues.process_start,
              'process_end': l_process_end,
              'linecount': 0,
              'rowcount': arg_count,
              'filedate': LocalValues.date_to_process,
              'status': arg_status_msg,
              'message': '',
              'raw_size': LocalValues.raw_size,
              'raw_replica_size': LocalValues.raw_replica_size,
              'refined_size': LocalValues.refined_size,
              'refined_replica_size' : LocalValues.refined_replica_size
               }

    utilities.GlobalValues.mySQLHandler_instance.perform_DML(arg_action=audit_action, arg_record=utilities.GlobalValues.record)


def main():
    start_time = timer()
    parser = optparse.OptionParser(usage="usage: %prog [options values]",
                                   version="%prog 2.0")

    parser.add_option('-d', '--dbname',
                      help='Database of Table to be fork lifted into Hive',
                      dest='db_name')
    parser.add_option('-t', '--table',
                      help='Table name to be fork lifted into Hive',
                      dest='table_name')
    parser.add_option('-f', '--fromDate',
                      help='Optional: Date used to filter rows from source and also in name of extract file',
                      dest='fromDate')
    parser.add_option('-e', '--endDate',
                      help='Optional: End date used to filter rows, from source, for a range'
                           'Passing this fetches rows between start and end dates, inclusive of the dates passed',
                      dest='endDate',
                      default=None)
    parser.add_option('-o', '--operator',
                      help='Optional: Operator to be used to Filter rows. Possible values: >, >=, <, <=, =, <>'
                           'Used only when a single date value is passed.',
                      dest='operator',
                      default='=')
    parser.add_option('-r', '--runSwitch',
                      help='Optional: Use this to run script with limited functionality. '
                           'Possible values: G to Generate Scripts '
                           '\t\t\t E to Execute Data Extraction scripts'
                           '\t\t\t L to Execute Load to Target scripts'
                           '\t\t\t Q to Execute Data Quality Check scripts'
                           '\t\t\t GELQ (default) does both of the above',
                      dest='runSwitch',
                      default='GEL')
    parser.add_option('-p', '--persistExtract',
                      help='Optional: Used only for Dev or Debugging'
                           'Set to N by default, will be set to Y only during dev calls so extracts generated are not lost',
                      dest='persistExtract',
                      default='N')

    current_os = system()
    if current_os != "Windows":
        dir_sep = "/"
        # utilities.check_if_running('extract2Hive')
    else:
        dir_sep = "\\"

    # ---------------------------------------- Parse Arguments -------------------------------------------
    (opts, args) = parser.parse_args()
    if (opts.db_name is None) or (opts.table_name is None) or (opts.fromDate is None):
        parser.print_help()
        exit(-1)

    # ---------------------------------------- Start Logging  -------------------------------------------
    base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + dir_sep
    log_dir = os.path.expanduser('~') + dir_sep + "log" + dir_sep
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = log_dir + opts.db_name + "." + opts.table_name + "." + opts.fromDate + "." + \
                   datetime.now().strftime("%Y%m%d%H%M%S") + ".log"
    logging.basicConfig(filename=log_filename, filemode='w', level=logging.INFO)
    logging.info("Logging started")

    utilities.print_info("Log Directory: " + log_filename)
    utilities.print_info("Base Directory is " + base_dir)

    # col_meta_file = base_dir + "config" + dir_sep + "ops_metadata_tbl_cols.txt"
    # tbl_meta_file = base_dir + "config" + dir_sep + "ops_metadata_tbl.txt"

    utilities.print_info("Current OS: " + current_os)
    # utilities.print_info("Table Metadata File: " + tbl_meta_file)
    # utilities.print_info("Column Metadata File: " + col_meta_file)

    # ---------------------------------------- Validate arguments -------------------------------------------
    if opts.db_name is None:
        utilities.abort_with_msg("Please provide Source Database Name")
    if opts.table_name is None:
        utilities.abort_with_msg("Please provide Source Table/View Name")
    if opts.fromDate is None:
        utilities.abort_with_msg("Please provide Date or Date Range to fetch data")

    operators_to_check = [">", ">=", "<", "<=", "=", "<>"]
    if opts.operator != '':
        if not any(oper in opts.operator for oper in operators_to_check):
            parser.print_help()
            utilities.abort_with_msg("Not a valid operator to build a condition")

    legal_switch = ["G", "GE", "GEL", "GL", "Q", "GQ", "GLQ", "GELQ", "GF", "GEF", "GELF", "GLF", "GQF", "GELQF", "GFE", "GEFL", "GEFLQ", "GEQLF", "GFEL"]
    if opts.runSwitch.upper() not in legal_switch:
        # if not any(switch == opts.runSwitch for switch in legal_switch):
        parser.print_help()
        utilities.abort_with_msg("Not a valid runSwitch. Valid switch combinations: " + str(legal_switch))

    if "G" not in opts.runSwitch and opts.runSwitch is not None:
        parser.print_help()
        utilities.abort_with_msg("Cannot execute Extraction, Load or Quality check without script generation. " +
                                 "Include G in switch")

    # -------------------- Store values into Class variable to avoid passing around --------------------------------
    LocalValues.db_nm = opts.db_name.strip().upper()
    LocalValues.tbl_nm = opts.table_name.strip().upper()
    LocalValues.date_to_process = opts.fromDate
    LocalValues.mysql_prop_file = base_dir + 'common' + dir_sep + 'ENV.mySQL.properties'
    LocalValues.process_start = datetime.now()
    print("Prop file: " + LocalValues.mysql_prop_file)
#    mySQL_helper(log_filename, LocalValues.mysql_prop_file)
    utilities.mySQLhdr = mySQLHandler(LocalValues.mysql_prop_file)

    utilities.GlobalValues.mySQLHandler_instance = mySQLHandler(LocalValues.mysql_prop_file)
    utilities.GlobalValues.epv_aim_file = base_dir + 'common' + dir_sep + 'ENV.epvaim.properties'

    # ---------------------------------------- Read Metadata into Arrays -------------------------------------------
    # Fetch Table attributes for Filtering, Partition & Distribution
    mysql_meta = mySQLMetadata(LocalValues.mysql_prop_file)
    tbl_meta = mysql_meta.read_table_metadata(sourceDB=LocalValues.db_nm, sourceTable=LocalValues.tbl_nm)
    col_meta = mysql_meta.readColumnMetadata(sourceDB=LocalValues.db_nm, sourceTable=LocalValues.tbl_nm)

    for key, val in sorted(tbl_meta.items()):
        utilities.print_info("\t\t" + key + " : " + str(val))
    #for col_meta_row in col_meta:
    #    utilities.print_info("\t\t" + "".join([str(tpl) for tpl in col_meta_row] ))

    # tbl_meta = utilities.read_file(arg_db_nm=LocalValues.db_nm, arg_tbl_nm=LocalValues.tbl_nm,
    #                                arg_input_file=tbl_meta_file, arg_content_type='TABLES')
    #
    # col_meta = utilities.read_file(arg_db_nm=LocalValues.db_nm, arg_tbl_nm=LocalValues.tbl_nm,
    #                                arg_input_file=col_meta_file, arg_content_type='COLS')

    # ---------------------------------------- Generate Scripts and DDL's -------------------------------------------
    ext_time = 0
    LocalValues.job_pid = 0
#   audit_action = 'insert'
    audit_action = set_audit_action()
    audit_log(audit_action,'OPS_GEN_SCRIPT INITIALIZED')

    if "G" in opts.runSwitch:  # Check for switch to Generate data extraction scripts

        LocalValues.job_pid = int(round(time() * 1000))
        if tbl_meta['delta_col']:  # Check for metadata to determine how to fetch deltas
            l_extract_filter = utilities.build_extract_filter(arg_tbl_meta=tbl_meta,
                                                              arg_from_val=opts.fromDate,
                                                              arg_to_val=opts.endDate,
                                                              arg_operator=opts.operator,
                                                              run_switch=opts.runSwitch)
        else:
            utilities.print_warn("No Condition Built for extract. Placeholder condition of 1=1 will be used")
            l_extract_filter = "1 = 1"

        # A bunch of Key Value Pairs used at the time of Running Scripts Generated

        # Extract column metadata attribute values

        col_filter_y = []
        for i, col_meta_row in enumerate(col_meta):
             if col_meta_row[6][1] == 'Y':
                 col_filter_y.append(col_meta_row)

        col_meta_values = []
        for i, col_meta_row in enumerate(col_filter_y):
            col_meta_values.append([])
            tuples = col_meta_row[2:len(col_meta_row)]
            for j, v in enumerate(tuples):
                col_meta_values[i].append(v[1])

        print 'debug 1'
        helper_dict = utilities.gen_script_from_tmplt(arg_base_dir=base_dir,
                                                      arg_tbl_meta=tbl_meta,
                                                      arg_col_meta=col_meta_values,
                                                      arg_dir_sep=dir_sep,
                                                      arg_xtract_filter=l_extract_filter,
                                                      arg_from_val=opts.fromDate,
                                                      arg_to_val=opts.endDate,
                                                      arg_operator=opts.operator,
                                                      run_switch=opts.runSwitch)
        audit_log(audit_action, 'GENERATE EXTRACT SCRIPT')
        print 'debug 1'
        gen_time = timer()
        utilities.print_info("Time taken to generate scripts " + str(gen_time - start_time) + " Seconds")
        rows = 0

    # ---------------------------------------- Run all extraction Scripts  -------------------------------------------
    if "E" in opts.runSwitch:  # Check for switch to execute data Extraction scripts

        audit_action = set_audit_action()
        audit_log(audit_action, 'START EXECUTE SOURCE DB EXTRACT SCRIPT', 0)
        extract_file, rows = utilities.run_extract(arg_tbl_meta=tbl_meta,
                              arg_log_dir=log_dir,
                              arg_passwd_file=base_dir + 'common' + dir_sep + 'ENV.scriptpwd.properties',
                              arg_date_for_extract=opts.fromDate,
                              arg_helper_dict=helper_dict)

        LocalValues.rowcount = rows
        audit_log(audit_action, 'COMPLETED EXECUTE SOURCE DB EXTRACT SCRIPT', rows, extract_file)

        # ---- V1 framework takes the extract and uses Shell scripts for further processing. Hence EXIT.-----
        if tbl_meta["v1_support"] == "Y":
            exit(0)

        # Run alter raw table to add partition
        utilities.run_hql(arg_script=helper_dict["alt_raw_tbl"],
                          arg_mode="f"
                          )
        ext_time = timer()
        utilities.print_info("Time taken to extract from Source " + str(ext_time - gen_time) + " Seconds")
        if extract_file != '' and opts.persistExtract == 'N':
            utilities.run_shell_cmd("rm -f " + extract_file)                # Delete extract file, if exists

#       added on 2/2/2017
        if tbl_meta["db_type"] == 'TERADATA':
            utilities.del_in_hdfs(tbl_meta["hdfs_basedir"] + "/" + tbl_meta["hdfs_extract_dir"] + "/" +
                                  os.path.basename(extract_file))

    if "L" in opts.runSwitch:                                 # Check for switch to execute data Load scripts

        audit_action = set_audit_action()
        # Run insert overwrite to refined tables
        utilities.run_hql(arg_script=helper_dict["ins_hive_rfnd_tbl"],
                          arg_mode="f",
                          arg_param=" --hiveconf inputsrcdt='" + opts.fromDate + "'"
                          )
        if '_RAW' in helper_dict["raw_hdfs_partition_location"].upper():
            v_dirpath=tbl_meta["hdfs_raw_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower()+"_raw"
        else:
            v_dirpath=tbl_meta["hdfs_raw_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower()

#       v_part_col = helper_dict["ptn_col_list"][0] + '_PTN' if helper_dict["ptn_col_list"][0] is not None else None

#       LocalValues.raw_size, LocalValues.raw_replica_size = utilities.check_hdfs_space(arg_hdfs_base_path=v_dirpath, part_col="LOADDATE", from_dt=opts.fromDate)
#       LocalValues.refined_size, LocalValues.refined_replica_size = utilities.check_hdfs_space(arg_hdfs_base_path=tbl_meta["hdfs_refined_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower(), part_col=v_part_col, from_dt=opts.fromDate, end_dt=opts.endDate )

        LocalValues.raw_size, LocalValues.raw_replica_size = 0, 0
        LocalValues.refined_size, LocalValues.refined_replica_size = 0, 0

        # Table Metadata contains data retention value.
        if tbl_meta["hive_raw_retention"].strip() != '' and tbl_meta["hive_raw_retention"].strip() != '0':
            purgepartitions.purge_partition(dirpath=tbl_meta["hdfs_raw_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower() + "_raw",
                                            retentiondays=tbl_meta["hive_raw_retention"],
                                            fromdt=opts.fromDate,
                                            ptncolnm="loaddate",
                                            hivedb=tbl_meta["stg_db"],
                                            hivetbl=tbl_meta["tgt_tbl"] + "_raw"
                                            )

        if tbl_meta["hive_refined_retention"].strip() != '' and tbl_meta["hive_refined_retention"].strip() != '0':
            purgepartitions.purge_partition(
                dirpath=tbl_meta["hdfs_refined_dir"] + dir_sep + tbl_meta["tgt_tbl"].lower(),
                retentiondays=tbl_meta["hive_refined_retention"],
                fromdt=opts.fromDate,
                ptncolnm=helper_dict["ptn_col_list"][0] + '_PTN',
                hivedb=tbl_meta["tgt_db"],
                hivetbl=tbl_meta["tgt_tbl"]
                )

        audit_log(audit_action, 'EXECUTE DATA LOAD', rows)

        load_time = timer()
        utilities.print_info("Time taken to load to Hive refined table " + str(load_time - ext_time) + " seconds")
        utilities.print_info(
            "Time taken to generate,extract and load to hive " + str(load_time - start_time) + " seconds")

    if "Q" in opts.runSwitch:  # Check for switch to execute data Quality scripts
        utilities.run_dq(arg_tbl_meta=tbl_meta,
                         arg_log_dir=log_dir,
                         from_date=opts.fromDate,
                         end_date=opts.endDate,
                         part_col=helper_dict["ptn_col_list"][0] + '_PTN',
                         which_table="both"
                         )
        qual_time = timer()
        utilities.print_info("Time taken for generate till quality check " + str(qual_time - start_time) + " seconds")

    audit_action = 'update'
    audit_log(audit_action, 'COMPLETED SUCCESSFULLY', LocalValues.rowcount)


if __name__ == "__main__":
    main()


