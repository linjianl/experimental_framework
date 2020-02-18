from __future__ import division, print_function
import logging
import os
import sys
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window
import pandas as pd

spark = ( SparkSession
             .builder
             .getOrCreate() )

sc = spark.sparkContext

from exp_lib.account.base import ExperimentBase, MetricBase

# add mysql access module
sys.path.append(os.path.join(os.getenv("HOME"),"git_tree/partners/python/utils/"))
import MetaUtils as mu

class account(ExperimentBase):
    """
    account_4 account, child class of experimentBase class, the child class specifies the account_4 account specific exp_weight,
    every other attributes inherit from experimentBase class

    Attributes:
        Metrics    : performance metrics data
        run_date   : string, the date that an experiment starts to run
        exp_groups : list, how much weight each exp_groups represent
        control    : list or integer, which experiment group(s) is control
        test       : list or integer, which experiment group(s) is test
        exp_weights: account specific experiemnt weight
        period     : integer, number of days to looks back to calculate the pre-experiment difference
        base_salt  : string, the first character of the salt
        ntrials    : integer, number of iterations
        pos        : string, point of sale, booker_country

    """
    def __init__(self,Metrics,run_date,exp_groups,control,test,exp_weights,
                 period,base_salt,ntrials,pos):

        self.exp_weights = [1] * 20
        super(account,self).__init__(Metrics,run_date,exp_groups,control,test,
                                             self.exp_weights,period,base_salt,ntrials,pos)

class NitsBookings(MetricBase):
    """
    Nits Bookings (per Property) class, child class of MetricBase class, the child class obtain the defined metrics for the relevant
    account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
        use_cuped   : boolean, use CUPED method or not
        cuped_period: integer, period for CUPED
    """

    def __init__(self,start_date,end_date,pos,use_cuped=False,cuped_period=None):

        self.metric_name = "nits_bookings"
        super(NitsBookings,self).__init__(self.metric_name,start_date,
              end_date,pos,use_cuped,cuped_period)

    def compute(self):

        accountFourStats = AccountFourStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( accountFourStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )

class NitsProfit(MetricBase):
    """
    NitsProfit (per Property) class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
        use_cuped   : boolean, use CUPED method or not
        cuped_period: integer, period for CUPED
    """

    def __init__(self,start_date,end_date,pos,use_cuped=False,cuped_period=None):

        self.metric_name = "nits_profit"
        super(NitsProfit,self).__init__(self.metric_name,start_date,
              end_date,pos,use_cuped,cuped_period)

    def compute(self):

        accountFourStats = AccountFourStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( accountFourStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )

class GrossBookings(MetricBase):
    """
    GrossBookings (per Property) class, child class of MetricBase class, the child class obtain the defined
    metrics for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
        use_cuped   : boolean, use CUPED method or not
        cuped_period: integer, period for CUPED
    """

    def __init__(self,start_date,end_date,pos,use_cuped=False,cuped_period=None):

        self.metric_name = "gross_bookings"
        super(GrossBookings,self).__init__(self.metric_name,start_date,
              end_date,pos,use_cuped,cuped_period)

    def compute(self):

        accountFourStats = AccountFourStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( accountFourStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )

class GrossProfit(MetricBase):
    """
    GrossProfit (per Property) class, child class of MetricBase class, the child class obtain the defined metrics
    for the relevant account

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country
        use_cuped   : boolean, use CUPED method or not
        cuped_period: integer, period for CUPED
    """

    def __init__(self,start_date,end_date,pos,use_cuped=False,cuped_period=None):

        self.metric_name = "gross_profit"
        super(GrossProfit,self).__init__(self.metric_name,start_date,
              end_date,pos,use_cuped,cuped_period)

    def compute(self):

        accountFourStats = AccountFourStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( accountFourStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )

class AccountFourStats(object):
    """
    AccountFourStats class, obtain data for relevant performance metrics

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        agg_on      : dimensions to aggregate on, default to hotel_id, yyyy_mm_dd for smart salt, the lowest
                      aggregation level is hotel_id, yyyy_mm_dd, pos
        pos         : list or string, point of sale, booker_country
        max_rpb     : float, max revenue per booking as cutoff point to remove bookings with uncommon values
                      (normal or fraudulent)
        partner_id  : int, partner_id for specific account
        performance_table: string, performance table for an account
        reservation_table: string, reservation table in mysql for cancellation data source
        affiliate_table:   string, affiliate table which is joined with reservation table on afilliate id
    """

    def __init__(self,start_date,end_date,pos=['All'],
                max_rpb = 3000.0, partner_id = 413084,
                performance_table = 'spmeta.account_4_performance',
                reservation_table = 'ppc_sp.SPReservation',
                affiliate_table = 'bp_slice.FlatAffiliate',
                agg_on = ['hotel_id', 'yyyy_mm_dd']):

        self.start_date = start_date
        self.end_date   = end_date
        self.pos        = [pos] if isinstance(pos, str) else pos
        self.max_rpb    = max_rpb
        self.partner_id = partner_id
        self.performance_table = performance_table
        self.reservation_table = reservation_table
        self.affiliate_table = affiliate_table
        self.agg_on = agg_on


    def get_stats_summary(self):
        """function to obtain performance stats for account_4 at desired aggregated dimensions

        Returns:
            spark dataframe with performance metrics of nits_bookings,gross_bookings,nits_profit,gross_profit,
            cost, clicks, nits and gross commission at desired aggregated dimensions.
        """
        perf_table = spark.table(self.performance_table)\
                            .where("yyyy_mm_dd between '{start_date}' and '{end_date}'"
                                     .format(start_date = self.start_date, end_date = self.end_date))\
                            .where("clicks > 0")\
                            .where("commission_expected_euro <= {max_rpb}".format(max_rpb = self.max_rpb))

        if self.pos == ['All']:
            perf_table = perf_table.groupBy(*self.agg_on)\
                                .agg(f.sum("nits_bookings").alias("nits_bookings")
                                    ,f.sum("commission_expected_euro").alias("nits_commission")
                                    ,f.sum("bookings").alias("gross_bookings")
                                    ,f.sum("commission_amount_euro").alias("gross_commission")
                                    ,f.sum("cost_euro").alias("cost")
                                    ,f.sum("clicks").alias("clicks")
                                    ,f.sum("roomnights").alias("roomnights"))\
                               .withColumn("nits_profit",f.expr("nits_commission-cost"))\
                               .withColumn("gross_profit", f.expr("gross_commission-cost"))
        else:
            filtered_pos = spark.createDataFrame(pd.DataFrame(data = self.pos,
                                                              columns = ["pos"]))

            perf_table = perf_table.join(filtered_pos, on = "pos", how = "inner")\
                                .groupBy(*self.agg_on)\
                                .agg(f.sum("nits_bookings").alias("nits_bookings")
                                    ,f.sum("commission_expected_euro").alias("nits_commission")
                                    ,f.sum("bookings").alias("gross_bookings")
                                    ,f.sum("commission_amount_euro").alias("gross_commission")
                                    ,f.sum("cost_euro").alias("cost")
                                    ,f.sum("clicks").alias("clicks")
                                    ,f.sum("roomnights").alias("roomnights"))\
                               .withColumn("nits_profit",f.expr("nits_commission-cost"))\
                               .withColumn("gross_profit", f.expr("gross_commission-cost"))

        return (perf_table)

    def get_cancellations(self):
        """get cancellation data from mysql at desired aggregated dimension and filter for the selected point of
        sales.

        Returns: spark dataframe with cancelled commission, cancelled bookings and cancelled roomnights at desired
                 aggregated dimension
        """
        msql = mu.getConnSql()
        msql.execute('USE ppc_sp')
        cancellation_query = """
            SELECT r.date_cancelled yyyy_mm_dd
                 , a.distribution pos
                 , CAST(coalesce(r.dest_id, r.hotel_id) AS INT) hotel_id
                 , CAST(sum(1) AS INT) cancellations
                 , sum(commission_amount_euro) cancelled_commission
                 , CAST(sum(roomnights) AS INT) cancelled_roomnights
             FROM {reservation_table} r force index (cancel)
             JOIN {affiliate_table} a on (a.affiliate_id = r.affiliate_id)
            WHERE r.date_cancelled >= '{start_date}'
              AND r.date_cancelled < '{end_date}'
              AND r.status not in ('fraudulent','test','unknown')
              AND r.partner_id = {account_4_partner_id}
            GROUP BY yyyy_mm_dd, pos, coalesce(r.dest_id, r.hotel_id)
            """.format(reservation_table = self.reservation_table,
                        affiliate_table = self.affiliate_table,
                        start_date = self.start_date,
                        end_date = self.end_date,
                        account_4_partner_id = self.partner_id)

        cancellations = pd.read_sql_query(cancellation_query, con=msql)
        msql.close()
        cancellations  = spark.createDataFrame(cancellations)

        cancellations_agg = cancellations.groupBy(*self.agg_on)\
                            .agg(f.sum("cancelled_commission").alias("cancelled_commission")
                                ,f.sum("cancelled_roomnights").alias("cancelled_roomnights")
                                ,f.sum("cancellations").alias("cancellations"))

        if self.pos == ['All']:
            return cancellations_agg
        else:
            filtered_pos = spark.createDataFrame(pd.DataFrame(data = self.pos, columns = ["pos"]))
            cancellations_agg = cancellations_agg.join(filtered_pos, on = "pos", how = "inner")
            return cancellations_agg

    def get_agg_stats(self):
        """function that join cancellations data with performance metrics at desired aggregated dimensions

        Returns:
            spark dataframe of performance stats at desired aggregated dimensions
        """
        cancellations_agg = self.get_cancellations()

        perf_table = self.get_stats_summary()

        agg_perf_table = perf_table.join(cancellations_agg, how = "outer", on = self.agg_on)\
                              .withColumn("asbooked_commission",f.expr("gross_commission-cancelled_commission"))\
                              .withColumn("asbooked_roomnights", f.expr("roomnights - cancelled_roomnights"))

        agg_perf_table = agg_perf_table.na.fill(0, subset = list(set(agg_perf_table.columns) -
                                                                 set([x for x in self.agg_on])))

        return (agg_perf_table)
