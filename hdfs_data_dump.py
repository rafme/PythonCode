from datetime import datetime
from datetime import timedelta, date
import optparse
from platform import system
from timeit import default_timer as timer
import sys
import logging
import os
import subprocess
import re

def checkRC(exitcode, message):
    ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    logging.debug(ts + ": [[ERROR]] " + message)
    sys.exit(exitcode)

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def run_shell_cmd(arg_shell_cmd):
    try:
        ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        logging.info(ts + ": [[INFO]] " + arg_shell_cmd)
        p=subprocess.Popen(arg_shell_cmd,stdout=subprocess.PIPE, stderr=subprocess.STDOUT,shell=True)
        (output, error) = p.communicate()
        rc = p.wait()
        if rc != 0 and error is not None:
            logging.info(ts + ": [[INFO]] Output from Shell Execution: " + output)
            logging.debug(ts + ": [[ERROR]] Shell execution failed with return_code " + str(rc) + " and error " + str(error))
        else:
            logging.info(ts + ": [[INFO]] Output from Shell Execution: " + output)
        return rc
    except Exception as err:
        logging.debug(ts + ": [[ERROR]] An exception of type " + type(err).__name__ + "occured. Arguments: " + str(err.args))
        checkRC(99,"Shell Execution Failed")

def getPwd(userid):
    with open(base_dir + dir_sep + "etl" + dir_sep + "common" + dir_sep + "ENV.scriptpwd.properties") as f:
        for line in f:
            if userid in line:
                userid=line.split(':')[1]
                password=line.split(':')[2]
        import base64
        return base64.b64decode(password)

def runPSQL(cmd):
    ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    vGPServer = os.environ['GPMASTER']
    vGPUser = os.environ['GPUSER']
    vGPDB = os.environ['GPDB']
    vGPPort = os.environ['GPPORT']
    vCmd = "psql -h " + vGPServer + " -U " + vGPUser + " -d " + vGPDB + " -p " + vGPPort + ' -e -c "' + cmd + '"'
    logging.info(ts + ": [[INFO]] The Execution String is " + vCmd)
    run_shell_cmd(vCmd)

def RemoveHDFSFiles(filepath):
    ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    del_hdfs_path = "hadoop fs -rm -R -skipTrash " + filepath + "/*"
    logging.info(ts + " File(s) to be Deleted : " + del_hdfs_path)
    run_shell_cmd(del_hdfs_path)

def runHQL(cmd):
    ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    l_shell_cmd = 'beeline -u $HIVEHOST -f ' + cmd + ' --hiveconf mapred.job.queue.name=$HIVEQUEUE'
    logging.info(ts + ": [[INFO]] HQL Command" + l_shell_cmd)
    run_shell_cmd(l_shell_cmd)

def runSearch(filename, text):
    with open(filename) as f:
        for line in f:
            if text in line:
                return line

def runCounts(log_filename):
    src_cnt_line = runSearch(log_filename, "INSERT")
    src_cnt = map(str.strip, src_cnt_line.split(" "))
    hive_cnt_line = runSearch(log_filename, "numRows=")
    hive_cnt = hive_cnt_line.split("=")
    trgt_cnt = map(str.strip, hive_cnt[3].split(","))
    return "Src Count: " + src_cnt[2] + "," + "Destination Count: " + trgt_cnt[0]

def main():
    global base_dir
    global dir_sep
    start_time = timer()
#    xpo = {'impression':'event_time::date', 'click':'event_time::date', 'activity':'event_time::date', 'meta_activity_tag':'meta_date', 'meta_browser':'meta_date',
#           'meta_buy':'meta_date', 'meta_campaign':'meta_date', 'meta_country':'meta_date', 'meta_creative':'meta_date', 'meta_creative_activity_tagmap':'meta_data',
#           'meta_creative_group':'meta_date', 'meta_delivery':'meta_date', 'meta_dma':'meta_date', 'meta_dyn_content':'meta_date', 'meta_os':'meta_date',
#           'meta_query_string':'meta_date', 'meta_region':'meta_date', 'meta_scenario':'meta_date', 'meta_segment':'meta_date'}

    xpo = {'meta_activity_tag':'meta_date'}

    insight = {'pv':'timestamp::date', 'pi':'timestamp::date', 'sess':'timestamp::date'}

    customer_profile = {'cust_profile':'file_date::date' }

    parser = optparse.OptionParser(usage="usage: %prog [options values]",
                                   version="%prog 2.0")

    parser.add_option('-f', '--FeedName',
                      help='Name of the Feed that needs to be executed. Currently supported ones are xpo and insight',
                      dest='vFeedName')
    parser.add_option('-s', '--StartDate',
                      help='Optional: Date used to download rows from Greenplum',
                      dest='vFromDate')
    parser.add_option('-e', '--EndDate',
                      help='Optional: End date used to restrict rows from Greenplum'
                           'Passing this fetches rows between start and end dates, inclusive of the dates passed',
                      dest='vEndDate',
                      default=None)

    current_os = system()
    if current_os != "Windows":
        dir_sep = "/"
    else:
        dir_sep = "\\"

    (opts, args) = parser.parse_args()
    base_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + dir_sep
    log_dir = os.path.expanduser('~') + dir_sep + "log" + dir_sep
    log_filename = log_dir + opts.vFeedName + "." + opts.vFromDate + "." + datetime.now().strftime("%Y%m%d%H%M%S") + ".log"
    hql_dir = os.path.expanduser('~') + dir_sep + "etl" + dir_sep + "hql" + dir_sep
    #hql_dir = os.path.expanduser('~') + dir_sep + "log" + dir_sep + "hql" + dir_sep
    logging.basicConfig(filename=log_filename, filemode='w', level=logging.DEBUG)
    logging.info("Logging started")
    ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    logging.info(ts + ": [[INFO]] Log FileName: " + log_filename)
    logging.info(ts + ": [[INFO]] Base Directory is " + base_dir)

    if opts.vFeedName is None:
        parser.print_help()
        checkRC("99","Please provide Source Database Name")
    if opts.vFromDate is None:
        parser.print_help()
        checkRC("99","Please provide a From date")

    vStartDt = datetime.strptime(opts.vFromDate, "%Y%m%d").date()
    if opts.vEndDate is not None:
        vEndDt = datetime.strptime(opts.vEndDate, "%Y%m%d").date()


    if opts.vFeedName.upper() == 'XPO':
        for single_date in daterange(vStartDt, vEndDt):
            vRunDate=single_date.strftime("%Y%m%d")
            for k, v in xpo.items():
                RemoveHDFSFiles("/tenants/opsdata/raw/xpo/" + k )
                vRunString = "insert into dops.ex_w_xpo_" + k + " select * from dm.intg_xpo_" + k + " where " + v + " = '"  +  vRunDate + "';"
                logging.info(ts + ": [[INFO]] Executing Command: " + vRunString)
                runPSQL(vRunString)
                runHQL(hql_dir + "xpo_" + k + ".hql")
                cnt_string = runCounts(log_filename)
                print("Feed:XPO => " + k + " => " + cnt_string)
    elif opts.vFeedName.upper() == "INSIGHT":
        for single_date in daterange(vStartDt, vEndDt):
            vRunDate=single_date.strftime("%Y%m%d")
            for k, v in xpo.items():
                vRunString = "insert into dops.ex_w_ins_" + k + " select * from dm.intg_ins_" + k + " where " + v + " = '"  +  vRunDate + "';"
                logging.info(ts + ": [[INFO]] Executing Command: " + vRunString)
                runPSQL(vRunString)
                runHQL(hql_dir + "insight_" + k + ".hql")
    elif opts.vFeedName.upper() == "CUST_PROFILE":
        for k, v in customer_profile.items():
             vRunDate=vStartDt.strftime("%Y%m%d")
             vRunString = "insert into dops.ex_w_cdim_" + k + " select * from dm.intg_cdim_" + k + " where " + v + " = '"  +  vRunDate + "';"
             logging.info(ts + ": [[INFO]] Executing Command: " + vRunString)
             runPSQL(vRunString)
             runHQL(hql_dir + "cust_profile.hql")

if __name__ == "__main__":
    main()

