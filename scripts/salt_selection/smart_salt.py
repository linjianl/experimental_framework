# /opt/spark/current20/bin/spark-submit smart_salt.py  --exp_groups 10 10 --ntrials 2000 --account trivago 2> /dev/null
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
import shutil
import subprocess as sb

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
    parser.add_argument("--pos", nargs = '+', type=str, default='All') 
    args = parser.parse_args()

    logging.info("Initialising with the following parameters:")
    logging.info(str(args))
    
    #  file zipping 
    current_dir = os.path.dirname(os.path.realpath(__file__))
    current_dir = current_dir[:current_dir.find('scripts')]
    mod_file = os.path.join(current_dir, "exp_lib/python/utils/zip_module.py")
    sc.addPyFile(mod_file)
    
    mod_zipping = importlib.import_module("zip_module")
    mod_zipping.zip_and_ship_module(current_dir)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    zipped_file = os.path.join(current_dir,"exp_lib.zip")
    sc.addPyFile(zipped_file)
    
    # important relevant module for the account 
    mod = importlib.import_module("exp_lib.account.{0}".format(args.account))
    Metrics = [mod.NitsBookings,mod.NitsProfit]
    logging.info("Optimising split on metrics: {}".format([x for x in Metrics]))
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

    salt, pae = experiment.get_smart_salt()
    logging.info("Salt that produces the best split: {0}".format(salt))
    logging.info("The corresponding relative difference is: {0}".format(pae))

    # remove the zipped file from directory 
    os.remove("exp_lib.zip")
    
if __name__ == "__main__":
    main()
