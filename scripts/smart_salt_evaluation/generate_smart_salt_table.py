# /opt/spark/current20/bin/spark-submit generate_smart_salt_table.py --exp_groups 2 2 2 2 2 2 2 2 2 2 --period 28  --account trivago --run_date 2019-03-01 --ntrials 10000 --group_size 10percent  2> /dev/null
from __future__ import division, print_function
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import logging
import importlib
import numpy as np
import argparse

from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window

NUM_PARTITIONS = 1024
spark = ( SparkSession
             .builder
             .config("spark.app.name","SmartSaltConfig")
             .config("spark.yarn.queue","root.sp_analytics.spark")
             .config("spark.default.serializer","org.apache.spark.serializer.KryoSerializer")
             .config("spark.speculation","true")
             .config("spark.default.parallelism",str(NUM_PARTITIONS))
             .config("spark.sql.shuffle.partitions",str(NUM_PARTITIONS))
             .config("spark.yarn.executor.memoryOverhead", str(8192))
             .enableHiveSupport()
             .getOrCreate() )

sc = spark.sparkContext

def main():

    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)-10s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", type=str)
    parser.add_argument("--run_date", type=str, default=str(datetime.today().date()))
    parser.add_argument("--exp_weights", nargs='+', type=int, default=[1]*10)
    parser.add_argument("--exp_groups", nargs='+', type=int, default=[1]*10)
    parser.add_argument("--base_salt", type=str, default="B")
    parser.add_argument("--control", nargs='+', type=int, default=0)
    parser.add_argument("--test", nargs='+', type=int, default=1)
    parser.add_argument("--period", type=int, default=42)
    parser.add_argument("--ntrials", type=int, default=2000)
    parser.add_argument("--pos", type=str, default='All') # only for trivago,tripadvisor for now, format: US,GB,RU
    parser.add_argument("--user_name", type=str, default='lijia')
    parser.add_argument("--group_size", type=str, default = '5percent')
    args = parser.parse_args()

    logging.info("Initialising with the following parameters:")
    logging.info(str(args))

    #  file zipping
    current_dir = os.path.dirname(os.path.realpath(__file__))
    current_dir = current_dir[:current_dir.find('scripts')]
    mod_file = os.path.join(current_dir, "exp_lib/python/utils/zip_module.py")
    sc.addPyFile(mod_file)

    mod_zipping = importlib.import_module("zip_module")
    files = os.listdir(os.path.dirname(os.path.realpath(__file__)))
    files.append('exp_lib.zip')
    mod_zipping.zip_and_ship_module(current_dir,excluded_files = files)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    zipped_file = os.path.join(current_dir,"exp_lib.zip")
    sc.addPyFile(zipped_file)

    # important relevant module for the account
    mod = importlib.import_module("exp_lib.account.{account_name}".format(account_name = args.account))
    Metrics = [mod.NitsBookings,mod.NitsProfit]
    logging.info("Optimising split on metrics: {metric_names}".format(metric_names = [x for x in Metrics]))
    experiment = mod.account(Metrics,
                             run_date=args.run_date,
                             exp_groups=args.exp_groups,
                             control=args.control,
                             test=args.test,
                             exp_weights = args.exp_weights,
                             period=args.period,
                             base_salt=args.base_salt,
                             ntrials = args.ntrials,
                             pos = args.pos)

    salt_table = experiment.get_smart_salt_table().cache()
    salt_table.registerTempTable("salt_table")

    table_name = ('_'.join([args.account, 'smart_salt_table', datetime.strptime(args.run_date, "%Y-%m-%d").strftime("%Y%m%d"),
                            "ntrials",str(args.ntrials), "group_size", str(args.group_size)]))
    logging.info("the table name is {table_name}".format(table_name = table_name))

    spark.sql("DROP TABLE IF EXISTS {user_name}.{table_name}".format(user_name = args.user_name, table_name = table_name))
    spark.sql("CREATE TABLE {user_name}.{table_name} as SELECT * FROM salt_table".format(user_name = args.user_name, 
                                                                                         table_name = table_name))
    logging.info("Number of rows in the {account_name} salt table of group size {group_size} of {ntrials} salts are : \
                 {nrow}".format(account_name = args.account, group_size = args.group_size, ntrials = args.ntrials, 
                                nrow = salt_table.count()))

    # remove the zipped file from directory
    os.remove("exp_lib.zip")

if __name__ == "__main__":
    main()
