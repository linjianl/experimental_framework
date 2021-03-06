# /opt/spark/current20/bin/spark-submit generate_bootstrapped_smart_salt_result.py --exp_groups 2 2 2 2 2 2 2 2 2 2 --account trivago --group_size 10percent --run_date 2019-02-28 --source_table_name trivago_smart_salt_table_20190201_ntrials_10000_group_size_10percent --number_of_salts 100  2>/dev/null

from __future__ import division, print_function
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import logging
import importlib
import numpy as np
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window
import argparse
from booking.spark.repartition import repartition_to_blocksize

NUM_PARTITIONS = 512

spark = ( SparkSession
             .builder
             .config("spark.app.name","BootstrapConfig")
             .config("spark.yarn.queue","root.sp_analytics.spark")
             .config("spark.default.serializer","org.apache.spark.serializer.KryoSerializer")
             .config("spark.speculation","true")
             .config("spark.sql.autoBroadcastJoinThreshold","-1")
             .config("spark.default.parallelism",str(NUM_PARTITIONS))
             .config("spark.sql.shuffle.partitions",str(NUM_PARTITIONS))
             .enableHiveSupport()
             .getOrCreate() )

sc = spark.sparkContext

def main():

    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)-10s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", type=str)
    parser.add_argument("--run_date", type=str, default=str(datetime.today().date()))
    parser.add_argument("--exp_weights", nargs='+', type=int, default=[1]*20)
    parser.add_argument("--exp_groups", nargs='+', type=int, default=[1]*20)
    parser.add_argument("--base_salt", type=str, default="B")
    parser.add_argument("--control", nargs='+', type=int, default=0)
    parser.add_argument("--test", nargs='+', type=int, default=0)
    parser.add_argument("--period", type=int, default=28)
    parser.add_argument("--ntrials", type=int, default=10000)
    parser.add_argument("--pos", type=str, default='All') # only for trivago,tripadvisor for now, format: US,GB,RU
    parser.add_argument("--user_name", type = str, default = "lijia")
    parser.add_argument("--group_size", type=str, default = '5percent')
    parser.add_argument("--source_table_name", type = str, default = 'trivago_smart_salt_table_20190301_ntrials_10000_group_size_50percent')
    parser.add_argument("--number_of_salts", type = int, default = 10)
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
    
    # import relevant module for the account
    mod = importlib.import_module("exp_lib.account.{0}".format(args.account))
    Metrics = [mod.NitsBookings,mod.NitsProfit]
    logging.info("Bootstrapping on metrics: {}".format([x for x in Metrics]))

    data = mod.account(Metrics,
                       run_date=args.run_date,
                       base_salt=args.base_salt,
                       ntrials = args.ntrials,
                       pos=args.pos,
                       period = args.period,
                       exp_groups = args.exp_groups,
                       control = args.control,
                       test = args.test,
                       exp_weights = args.exp_weights)

    bootstrapped_table = data.get_bootstrapped_smart_salt_performance(source_table_name = args.source_table_name,
                                                                     exp_weights = args.exp_weights,
                                                                     number_of_salts = args.number_of_salts).cache()
    repartition_to_blocksize(bootstrapped_table).registerTempTable("bootstrapped_table")

    table_name = ('_'.join([args.account, 'smart_salt_bootstrapping',
                            datetime.strptime(args.run_date, "%Y-%m-%d").strftime("%Y%m%d"), 
                            args.group_size,'salt_numbers', str(args.number_of_salts)]))
    logging.info("the table name is {0}".format(table_name))

    spark.sql("DROP TABLE IF EXISTS {0}.{1}".format(args.user_name, table_name))
    spark.sql("CREATE TABLE {0}.{1} as SELECT * FROM bootstrapped_table".format(args.user_name, table_name))
    logging.info("Number of rows in the {0} bootstrapping table is : {1}".format(args.account, bootstrapped_table.count()))
    os.remove('exp_lib.zip')
    
if __name__ == "__main__":
    main()

