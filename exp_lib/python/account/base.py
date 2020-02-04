from __future__ import division, print_function
import pandas as pd
import os
import hashlib
import logging
import numpy as np
from statsmodels.tsa.stattools import adfuller, kpss

from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window, Row
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

from exp_lib.utils.get_perf_stats import get_perf_metrics_stats, create_stationarity_udf, create_bootstrapped_udf, create_loss_udf, create_loss_udf_v2, create_loss_udf_v3
from exp_lib.utils.days_setting import minusndays
from exp_lib.utils.get_salt_table import get_salt_table


class ExperimentBase(object):
    """base class for getting experiment salts and empirical distribution of an A/A testing

    Attributes:
        Metrics    : performance metrics data
        run_date   : string, the date that an experiment starts to run
        exp_groups : list, how much weight each exp_groups represent
        control    : list or integer, which experiment group(s) is control
        test       : list or integer, which experiment group(s) is test
        exp_weights: list, what is the minimium weight per group
        period     : integer, number of days to looks back to calculate the pre-experiment difference
        base_salt  : string, the first character of the salt
        ntrials    : integer, number of iterations
        pos        : string or list, point of sale, booker_country
    """

    def __init__(self,Metrics,run_date,exp_groups,control,test,exp_weights,
                 period,base_salt,ntrials,pos):

        self.run_date    = run_date
        self.period      = period
        self.ntrials     = ntrials

        self.start_date = minusndays(self.run_date,self.period)
        self.end_date   = minusndays(self.run_date,1)

        self.exp_weights = exp_weights
        self.exp_groups  = exp_groups
        self.control     = [control] if isinstance(control, int) else control
        self.test        = [test] if isinstance(test, int) else test

        self.base_salt   = base_salt
        self.pos         = [pos] if isinstance(pos, str) else pos
        self.Metrics = [Metric(self.start_date,self.end_date,self.pos) for Metric in Metrics]
        self.metric_names = [m.metric_name for m in self.Metrics]

    def get_smart_salt(self):
        """ ExperiemntBase Class Methods to obtain the best smart salt that
        (1) minimizes the relative difference between intended test control groups for 2 metrics bookings and profit per hotel
        (2) passes the stationarity(level) test (both KPSS tests and ADF tests) to fulfill the assumption that the relatie difference time
        series between test and control groups is stable until the experiment start and the only exogenous shock is the experiment

        Returns:
            the best salt and the corresponding relative difference of 2 metrics
        """
        # 1. getting the relevant metrics performance stats
        perf = get_perf_metrics_stats(Metrics = self.Metrics, metric_names = self.metric_names)

        # 2. get the salt table
        salt_table = get_salt_table(base_salt = self.base_salt, run_date = self.run_date, ntrials = self.ntrials)

        # 3. calculate the relative difference

        logging.info("the final experiment weights for this account is {0}".format(self.exp_weights))
        loss_udf = create_loss_udf(metric_names=self.metric_names,control=self.control,test=self.test,
                          perf=perf.value,exp_groups=self.exp_groups,period=self.period,weights = self.exp_weights)

        split_loss = ( salt_table.withColumn("pae", loss_udf(f.col('salt')))
                        .orderBy("pae").cache() )

        logging.info('''Complete loss function calculation, the best salt that minimize the relative difference is {0} and the relative
                      difference is {1} '''.format(split_loss.first()["salt"], split_loss.first()["pae"]))

        # 4. do batch processing to find the salt with minimum distance that passes the stationarity test
        # create indexing
        logging.info("Start processing stationarity testing")
        new_schema = t.StructType(split_loss.schema.fields[:] + [t.StructField("index", t.LongType(), False)])
        zipped_rdd = split_loss.rdd.zipWithIndex()
        row_with_index = Row(','.join(split_loss.columns) + ",index")
        split_loss_indexed = (zipped_rdd.map(lambda ri: row_with_index(*list(ri[0]) + [ri[1]])).toDF(new_schema))

        stationarity_udf = create_stationarity_udf(metric_names = self.metric_names, control = self.control, test = self.test,
                                       perf = perf.value, exp_groups = self.exp_groups, weights = self.exp_weights)

        # one batch = 500 salts
        batch_size = 500
        n_round = int(self.ntrials/batch_size)
        for i in range(n_round):
            index_upper = i * batch_size + batch_size
            index_lower = i * batch_size
            logging.info("The lower index is {0} and the higher index is {1}".format(index_lower, index_upper))
            selected_salt_table = ( split_loss_indexed.filter(split_loss_indexed.index < index_upper)
                                   .filter(split_loss_indexed.index >= index_lower) )
            selected_salt_table = ( selected_salt_table.withColumn("stat_result", stationarity_udf(f.col("salt")))
                                   .where("stat_result == True").cache() )
            if len(selected_salt_table.head(1)) > 0:
                best_salt = selected_salt_table.first()
                logging.info("There is a salt with minimum relative difference that passes the stationarity test:")
                break

        try:
            return best_salt["salt"], best_salt["pae"]*100
        except NameError:
            best_split = split_loss.first()
            logging.info("There is no salt that passes the stationarity test, the best salt is:")
            return best_split["salt"], best_split["pae"]*100

    def get_bootstrapped(self, exp_weights = [1] * 20):
        """ ExperiemntBase Class Methods to obtain the empirical distribution of relevant metrics (profit and bookings) to detect the
        inherent variation by randomly choosing groups of hotels to compare.

        Args:
            exp_weights: minimum weight per group, default to 20 groups
        Returns:
            the table with bootstrapped relative difference metrics across various sizes of groups and a number of days
        """

        # 1. get the relevant metrics performance stats
        perf = get_perf_metrics_stats(self.Metrics, self.metric_names)

        # 2. get the salt table
        salt_table = get_salt_table(base_salt = self.base_salt, run_date = self.run_date, ntrials = self.ntrials)

        # 3. bootrapping
        logging.info("start relative difference calculation")
        bootstrapped_udf = create_bootstrapped_udf(metric_names= self.metric_names, perf=perf.value, period=self.period,
                                                  weights = exp_weights)
        logging.info("the experiment weights are {0}".format(exp_weights))

        rel_diff_table = ( salt_table.withColumn("result",bootstrapped_udf(f.col('salt')))
                          .selectExpr("salt", "explode(result) result_tuple")
                          .selectExpr("salt",
                                      "result_tuple['paired_groups'] paired_groups",
                                      "result_tuple['num_days'] num_days",
                                      "result_tuple['num_groups'] num_groups",
                                      "result_tuple['rdiff_metric_1'] rdiff_{0}".format(self.metric_names[0]),
                                      "result_tuple['rdiff_metric_2'] rdiff_{0}".format(self.metric_names[1]))
                          )

        logging.info("complete with the table")

        return rel_diff_table
    
    def get_smart_salt_table(self):
        """ ExperiemntBase Class Methods to obtain a smart table with at least 100 salts that
        (1) minimizes the relative difference between intended test control groups for 2 metrics bookings and profit per hotel
        (2) passes the stationarity(level) test (both KPSS tests and ADF tests) to fulfill the assumption that the relatie difference time
        series between test and control groups is stable until the experiment start and the only exogenous shock is the experiment

        Returns:
            panda dataframe
        """
        # 1. getting the relevant metrics performance stats
        perf = get_perf_metrics_stats(Metrics = self.Metrics, metric_names = self.metric_names)

        # 2. get the salt table
        salt_table = get_salt_table(base_salt = self.base_salt, run_date = self.run_date, ntrials = self.ntrials)

        # 3. calculate the relative difference

        logging.info("the final experiment weights for this account is {0}".format(self.exp_weights))
        loss_udf = create_loss_udf(metric_names=self.metric_names,control=self.control,test=self.test,
                          perf=perf.value,exp_groups=self.exp_groups,period=self.period,weights = self.exp_weights)

        split_loss = ( salt_table.withColumn("pae", loss_udf(f.col('salt')))
                        .orderBy("pae").cache() )

        logging.info('''Complete loss function calculation, the best salt that minimize the relative difference is {0} and the relative
                      difference is {1} '''.format(split_loss.first()["salt"], split_loss.first()["pae"]))

        # 4. do batch processing to find the salt with minimum distance that passes the stationarity test
        # create indexing
        logging.info("Start processing stationarity testing")
        new_schema = t.StructType(split_loss.schema.fields[:] + [t.StructField("index", t.LongType(), False)])
        zipped_rdd = split_loss.rdd.zipWithIndex()
        row_with_index = Row(','.join(split_loss.columns) + ",index")
        split_loss_indexed = (zipped_rdd.map(lambda ri: row_with_index(*list(ri[0]) + [ri[1]])).toDF(new_schema))

        stationarity_udf = create_stationarity_udf(metric_names = self.metric_names, control = self.control, test = self.test,
                                       perf = perf.value, exp_groups = self.exp_groups, weights = self.exp_weights)

        # one batch = 500 salts
        batch_size = 500
        n_round = int(self.ntrials/batch_size)
        for i in range(n_round):
            index_upper = i * batch_size + batch_size
            index_lower = i * batch_size
            logging.info("The lower index is {0} and the higher index is {1}".format(index_lower, index_upper))
            selected_salt_table = ( split_loss_indexed.filter(split_loss_indexed.index < index_upper)
                                   .filter(split_loss_indexed.index >= index_lower) )
            selected_salt_table = ( selected_salt_table.withColumn("stat_result", stationarity_udf(f.col("salt")))
                                   .where("stat_result == True").cache() )
            if selected_salt_table.count() >= 100:
                logging.info("There is a salt table with at least 100 salts that passes the stationarity testing")
                break

        return selected_salt_table

    def get_bootstrapped_smart_salt_performance(self, exp_weights, source_table_name, number_of_salts):
        """ ExperiemntBase Class Methods to obtain the empirical distribution of relevant metrics (profit and bookings) for
        those salts that are chosen to minimize the pre-experiment bias.

        Args:
            exp_weights: minimum weight per group, default to 20 groups
        Returns:
            the table with bootstrapped relative difference metrics for a fixed size and a number of days
        """
        logging.info("start date is {0}".format(self.start_date))
        logging.info("end_date is {0}".format(self.end_date))
        
        # 1. get the relevant metrics performance stats
        perf = get_perf_metrics_stats(self.Metrics, self.metric_names)

        # 2. get the salt table
        salt_table = spark.sql("select salt from lijia.{0}".format(source_table_name)).limit(number_of_salts)
        logging.info("number of rows in salt table is {0}".format(salt_table.count()))

        # 3. bootrapping
        logging.info("start relative difference calculation")
        bootstrapped_udf = create_bootstrapped_udf(metric_names= self.metric_names, perf=perf.value, period=self.period,
                                                  weights = exp_weights, exp_groups_list = [self.exp_groups])
        logging.info("the experiment weights are {0}".format(exp_weights))

        rel_diff_table = ( salt_table.withColumn("result",bootstrapped_udf(f.col('salt')))
                          .selectExpr("salt", "explode(result) result_tuple")
                          .selectExpr("salt",
                                      "result_tuple['paired_groups'] paired_groups",
                                      "result_tuple['num_days'] num_days",
                                      "result_tuple['num_groups'] num_groups",
                                      "result_tuple['rdiff_metric_1'] rdiff_{0}".format(self.metric_names[0]),
                                      "result_tuple['rdiff_metric_2'] rdiff_{0}".format(self.metric_names[1]))
                          )

        logging.info("complete with the table")

        return rel_diff_table
    
    
    def get_smart_salt_performance(self):
        """ ExperiemntBase Class Methods to obtain the performance of those pre-selected by the smart salt procedure to check
        how much variance does these supposedly smarter salts generates. 

        Returns:
            spark dataframe
        """
        # 1. getting the relevant metrics performance stats
        perf = get_perf_metrics_stats(Metrics = self.Metrics, metric_names = self.metric_names)

        # 2. get the salt table
        salt_table = spark.sql("select salt from lijia.{0}".format('_'.join(['trivago', 'smart_salt_table', '20190301',
                                "ntrials",str(10000)]))).limit(100)
        logging.info("number of rows in salt table is {0}".format(salt_table.count()))

        # 3. calculate the relative difference

        logging.info("the final experiment weights for this account is {0}".format(self.exp_weights))
        loss_udf = create_loss_udf(metric_names=self.metric_names,control=self.control,test=self.test,
                          perf=perf.value,exp_groups=self.exp_groups,period=self.period,weights = self.exp_weights)

        split_loss = ( salt_table.withColumn("pae", loss_udf(f.col('salt')))
                        .orderBy("pae").cache() )

        logging.info('''Complete loss function calculation, the best salt that minimize the relative difference is {0} and the relative
                      difference is {1} '''.format(split_loss.first()["salt"], split_loss.first()["pae"]))

        return split_loss
    
    def get_random_salt_performance(self):
        """ ExperiemntBase Class Methods to obtain the performance of random salts to check
        how much variance does these supposedly salts generates. 

        Returns:
            spark dataframe
        """
        # 1. getting the relevant metrics performance stats
        perf = get_perf_metrics_stats(Metrics = self.Metrics, metric_names = self.metric_names)

        # 2. get the salt table
        salt_table = get_salt_table(base_salt = self.base_salt, run_date = self.run_date, ntrials = self.ntrials)

        # 3. calculate the relative difference
        logging.info("the final experiment weights for this account is {0}".format(self.exp_weights))
        loss_udf = create_loss_udf(metric_names=self.metric_names,control=self.control,test=self.test,
                          perf=perf.value,exp_groups=self.exp_groups,period=self.period,weights = self.exp_weights)

        split_loss = ( salt_table.withColumn("pae", loss_udf(f.col('salt')))
                        .orderBy("pae").cache() )

        logging.info('''Complete loss function calculation, the best salt that minimize the relative difference is {0} and the relative
                      difference is {1} '''.format(split_loss.first()["salt"], split_loss.first()["pae"]))

        return split_loss
    
    def get_smart_salt_table_v2(self):
        """ ExperiemntBase Class Methods to obtain the best smart salt v2 (does not divide by total number of groups) that
        (1) minimizes the relative difference between intended test control groups for 2 metrics bookings and profit per hotel
        (2) passes the stationarity(level) test (both KPSS tests and ADF tests) to fulfill the assumption that the relatie difference time
        series between test and control groups is stable until the experiment start and the only exogenous shock is the experiment

        Returns:
            the best salt and the corresponding relative difference of 2 metrics
        """
        # 1. getting the relevant metrics performance stats
        perf = get_perf_metrics_stats(Metrics = self.Metrics, metric_names = self.metric_names)

        # 2. get the salt table
        salt_table = get_salt_table(base_salt = self.base_salt, run_date = self.run_date, ntrials = self.ntrials)

        # 3. calculate the relative difference
        logging.info("the final experiment weights for this account is {0}".format(self.exp_weights))
        loss_udf = create_loss_udf_v2(metric_names=self.metric_names,control=self.control,test=self.test,
                          perf=perf.value,exp_groups=self.exp_groups,period=self.period,weights = self.exp_weights)

        split_loss = ( salt_table.withColumn("pae", loss_udf(f.col('salt')))
                        .orderBy("pae").cache() )

        logging.info('''Complete loss function calculation, the best salt that minimize the relative difference is {0} and the relative
                      difference is {1} '''.format(split_loss.first()["salt"], split_loss.first()["pae"]))

        # 4. do batch processing to find the salt with minimum distance that passes the stationarity test
        # create indexing
        logging.info("Start processing stationarity testing")
        new_schema = t.StructType(split_loss.schema.fields[:] + [t.StructField("index", t.LongType(), False)])
        zipped_rdd = split_loss.rdd.zipWithIndex()
        row_with_index = Row(','.join(split_loss.columns) + ",index")
        split_loss_indexed = (zipped_rdd.map(lambda ri: row_with_index(*list(ri[0]) + [ri[1]])).toDF(new_schema))

        stationarity_udf = create_stationarity_udf(metric_names = self.metric_names, control = self.control, test = self.test,
                                       perf = perf.value, exp_groups = self.exp_groups, weights = self.exp_weights)

        # one batch = 500 salts
        batch_size = 500
        n_round = int(self.ntrials/batch_size)
        for i in range(n_round):
            index_upper = i * batch_size + batch_size
            index_lower = i * batch_size
            logging.info("The lower index is {0} and the higher index is {1}".format(index_lower, index_upper))
            selected_salt_table = ( split_loss_indexed.filter(split_loss_indexed.index < index_upper)
                                   .filter(split_loss_indexed.index >= index_lower) )
            selected_salt_table = ( selected_salt_table.withColumn("stat_result", stationarity_udf(f.col("salt")))
                                   .where("stat_result == True").cache() )
            if selected_salt_table.count() >= 100:
                logging.info("There is a salt table with at least 100 salts that passes the stationarity testing")
                break

        return selected_salt_table
    
    def get_smart_salt_table_v3(self):
        """ ExperiemntBase Class Methods to obtain the best smart salt v2 (inverse variance weighted) that
        (1) minimizes the relative difference between intended test control groups for 2 metrics bookings and profit per hotel
        (2) passes the stationarity(level) test (both KPSS tests and ADF tests) to fulfill the assumption that the relatie difference time
        series between test and control groups is stable until the experiment start and the only exogenous shock is the experiment

        Returns:
            the best salt and the corresponding relative difference of 2 metrics
        """
        # 1. getting the relevant metrics performance stats
        perf = get_perf_metrics_stats(Metrics = self.Metrics, metric_names = self.metric_names)

        # 2. get the salt table
        salt_table = get_salt_table(base_salt = self.base_salt, run_date = self.run_date, ntrials = self.ntrials)

        # 3. calculate the relative difference
        logging.info("the final experiment weights for this account is {0}".format(self.exp_weights))
        loss_udf = create_loss_udf_v3(metric_names=self.metric_names,control=self.control,test=self.test,
                          perf=perf.value,exp_groups=self.exp_groups,period=self.period,weights = self.exp_weights)

        split_loss = ( salt_table.withColumn("pae", loss_udf(f.col('salt')))
                        .orderBy("pae").cache() )

        logging.info('''Complete loss function calculation, the best salt that minimize the relative difference is {0} and the relative
                      difference is {1} '''.format(split_loss.first()["salt"], split_loss.first()["pae"]))

        # 4. do batch processing to find the salt with minimum distance that passes the stationarity test
        # create indexing
        logging.info("Start processing stationarity testing")
        new_schema = t.StructType(split_loss.schema.fields[:] + [t.StructField("index", t.LongType(), False)])
        zipped_rdd = split_loss.rdd.zipWithIndex()
        row_with_index = Row(','.join(split_loss.columns) + ",index")
        split_loss_indexed = (zipped_rdd.map(lambda ri: row_with_index(*list(ri[0]) + [ri[1]])).toDF(new_schema))

        stationarity_udf = create_stationarity_udf(metric_names = self.metric_names, control = self.control, test = self.test,
                                       perf = perf.value, exp_groups = self.exp_groups, weights = self.exp_weights)

        # one batch = 500 salts
        batch_size = 500
        n_round = int(self.ntrials/batch_size)
        for i in range(n_round):
            index_upper = i * batch_size + batch_size
            index_lower = i * batch_size
            logging.info("The lower index is {0} and the higher index is {1}".format(index_lower, index_upper))
            selected_salt_table = ( split_loss_indexed.filter(split_loss_indexed.index < index_upper)
                                   .filter(split_loss_indexed.index >= index_lower) )
            selected_salt_table = ( selected_salt_table.withColumn("stat_result", stationarity_udf(f.col("salt")))
                                   .where("stat_result == True").cache() )
            if selected_salt_table.count() >= 100:
                logging.info("There is a salt table with at least 100 salts that passes the stationarity testing")
                break

        return selected_salt_table
    
class MetricBase(object):
    """base class for getting data for relevant performance metrics

    Attributes:
        metric_name : string, performance metrics data
        start_date  : string, starting date for getting the performance data
        end_date    : string, ending date for getting the performance data
        pos         : string, point of sale, booker country
        use_cuped   : boolean, use CUPED method or not
        cuped_period: integer, period for CUPED
    """

    def __init__(self,metric_name,start_date,end_date,pos,use_cuped,cuped_period):

        self.metric_name  = metric_name
        self.start_date   = start_date
        self.end_date     = end_date
        self.pos          = pos
        self.use_cuped    = use_cuped
        self.cuped_period = cuped_period

    @classmethod
    def new_obj(cls,name,exp_uvi,run_date=None):
        return cls(name,exp_uvi,run_date,use_cuped=False,metric_col=None)

    def compute(self):
        raise NotImplementedError

    @property
    def values(self):
        if self.use_cuped:
            return self.compute_w_cuped()
        else:
            return self.compute()

    def _calculate_theta(self,metrics):

        for col in self.metric_col:
            cov_metric     = metrics.stat.cov("preExp_"+col,col)
            var_pre_metric = metrics.stat.cov("preExp_"+col,"preExp_"+col)
            coeff          = cov_metric / var_pre_metric
            metrics        = metrics.withColumn("cuped_{}".format(col),
                               f.expr("{0}-{1}*preExp_{0}".format(col,str(coeff))) )

        return metrics

    @property
    def _exp_uvi_cuped(self):

        def shift_X(dd,X):
            return str((datetime.strptime(dd,"%Y-%m-%d") -
                        timedelta(X)).date())

        new_exp_uvi = \
          ( self.exp_uvi
                .withColumn("start_date",f.expr("date_sub(experiment_start_date,{}*7)"
                                                .format(cuped_period))) )

        return new_exp_uvi

    def apply_cuped(self):
        """
            this cuped is using stay date level metric average
            over multiple weekends
        """
        def shift_X(dd,X):
            return str((datetime.strptime(dd,"%Y-%m-%d") -
                                timedelta(X)).date())
        cuped_run_date = None
        if self.run_date:
            cuped_run_date = shift_X(self.run_date,self.shift_week*7)

        prev_metric = self.new_obj(self.name+"_cuped",self.__exp_uvi_cuped,cuped_run_date)
        pcol = ["exp_name","hotel_id"]
        colsExpr = ["{col} as prev_{col}".format(col=col) for col in self.metric_col]+pcol

        metrics = ( (self.compute())
                        .join(prev_metric.values.selectExpr(colsExpr),on=pcol,how="left")
                        .fillna(dict(zip(self.metric_col,[0]*len(self.metric_col))))
                   )

        cuped_metric = self.get_theta(metrics)

        return cuped_metric
