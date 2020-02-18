from __future__ import division, print_function
import logging
import os
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window

spark = ( SparkSession
             .builder
             .getOrCreate() )

sc = spark.sparkContext

from exp_lib.account.base import ExperimentBase, MetricBase

class account(ExperimentBase):
    """account_2 account, child class of experimentBase class, the child class specifies the account_2 account specific exp_weight,
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

        self.exp_weights = [1] * 10
        super(account, self).__init__(Metrics,run_date,exp_groups,control,test,self.exp_weights,period,
                                      base_salt,ntrials,pos)

class NitsBookings(MetricBase):
    """Nits Bookings (per Property) class, child class of MetricBase class, the child class obtain the defined
    metrics for the relevant account

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

        accountTwoStats = AccountTwoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( accountTwoStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )

class NitsProfit(MetricBase):
    """Nits Profit (per Property) class, child class of MetricBase class, the child class obtain the defined
    metrics for the relevant saccount

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

        accountTwoStats = AccountTwoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( accountTwoStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )

class GrossBookings(MetricBase):
    """Gross Bookings (per Property) class, child class of MetricBase class, the child class obtain the defined
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

        accountTwoStats = AccountTwoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( accountTwoStats.get_stats_summary()
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

        accountTwoStats = AccountTwoStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( accountTwoStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )

class AccountTwoStats(object):
    """
    AccountTwoStats class, obtain data for relevant performance metrics and also cancellations data, currently doe snot
    allow for filtering for pos

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string or list, point of sale, booker_country
        max_rpb     : float, max revenue per booking as cutoff point to remove bookings with uncommon values
                      (normal or fraudulent) for smart salt calculation, this is not necessary when calculating
                      for daily performance
        partner_id  : integer, relevant partner id for the specific account
        agg_on      : dimension to aggregate on
        nits_score_table: string, more reliable nits score table
        nits_postbook_table: string, postbook (less reliable) NITS table
        web_costs_table : string, relevant cost table for the specific account, account_2 separates web(desktop) and
                          mobile
        mobile_costs_table: string, relevant cost table for the specific account
        reservation_table: string, best source of reservation table
        affiliate_table: string, affiliate table which is joined with reservation table on afilliate id
    """

    def __init__(self,start_date,end_date,pos = ['All'],max_rpb=3000.0,partner_id = 431843,
                devices= ['web', 'mobile'],
                agg_on = ["hotel_id", "yyyy_mm_dd"],
                web_costs_table = 'spmeta.account_2_stats_coreweb',
                mobile_costs_table = 'spmeta.account_2_stats_coremobile',
                reservation_table = 'default.dw_reservation',
                nits_score_table = 'nits.scores',
                nits_postbook_table = 'default.reslog_nits_ng_postbooking_score',
                affiliate_table = 'default.bp_b_affiliate'):

        self.start_date = start_date
        self.end_date   = end_date
        self.pos        = pos if isinstance(pos, list) else [pos]
        self.max_rpb    = max_rpb
        self.partner_id = partner_id
        self.devices    = devices
        self.agg_on     = agg_on
        self.web_costs_table = web_costs_table
        self.mobile_costs_table = mobile_costs_table
        self.reservation_table = reservation_table
        self.nits_score_table = nits_score_table
        self.nits_postbook_table = nits_postbook_table
        self.affiliate_table = affiliate_table

    def get_stats_summary(self):
        """function to obtain performance stats for account_2.

        Returns:
            spark dataframe with metrics such as yyyy_mm_dd, hotel_id,
            nits_bookings,gross_bookings,nits_profit,gross_profit
        """

        web_clicks_query = """
        SELECT hotel_id,
               yyyy_mm_dd,
               locale,
               '{device_type}'                               AS device,
               Sum(Nvl(clicks_core, 0) + Nvl(clicks_bob, 0)) AS clicks,
               Sum(Nvl(cost_core, 0.) + Nvl(cost_bob, 0.))   AS cost
        FROM   {cost_table}
        WHERE  yyyy_mm_dd >= '{start_date}'
           AND yyyy_mm_dd <= '{end_date}'
           AND hotel_id IS NOT NULL
        GROUP  BY hotel_id,
                  yyyy_mm_dd,
                  locale
        """.format(device_type = self.devices[0], start_date = self.start_date,
                   end_date = self.end_date, cost_table = self.web_costs_table)

        web_clicks = spark.sql(web_clicks_query)

        mobile_clicks_query = """
        SELECT hotel_id,
               yyyy_mm_dd,
               locale,
               '{device_type}'                               AS device,
               Sum(Nvl(clicks_core, 0) + Nvl(clicks_bob, 0)) AS clicks,
               Sum(Nvl(cost_core, 0.) + Nvl(cost_bob, 0.))   AS cost
        FROM   {cost_table}
        WHERE  yyyy_mm_dd >= '{start_date}'
           AND yyyy_mm_dd <= '{end_date}'
           AND hotel_id IS NOT NULL
        GROUP  BY hotel_id,
                  yyyy_mm_dd,
                  locale
        """.format(device_type = self.devices[1], start_date = self.start_date,
                   end_date = self.end_date, cost_table = self.mobile_costs_table)

        mobile_clicks = spark.sql(mobile_clicks_query)

        clicks = web_clicks.union(mobile_clicks)

        reservation_query = """
        SELECT r.id hotelreservation_id
               , coalesce(cast(regexp_extract(r.label, 'hotel[-](.*)[_]xqdz', 1) as int), r.hotel_id) hotel_id
               , to_date(r.created) yyyy_mm_dd
               , r.affiliate_id
               , (case when length(regexp_extract(r.label, '[-]core(.*)[-]hotel', 1)) > 0
                        then upper(regexp_extract(r.label, '[-]core(.*)[-]hotel', 1))
                        else 'US'
                        end) locale
               , (case when r.label rlike '(linkmob[-]core.*[-]hotel)' then 'mobile'
                       when a.affiliate_name rlike '(Mobile)' then 'mobile'
                       else 'web'
                       end) device
                , 1 bookings
                , r.commission_amount_euro commission
                , roomnights
        FROM {reservation_table} r
        JOIN (
            SELECT id affiliate_id
                   , name affiliate_name
              FROM {affiliate_table}
             WHERE partner_id = {account_2_partner_id}
               AND name rlike '([_][Cc]ore[.])'
               ) a
          ON r.affiliate_id = a.affiliate_id
        WHERE to_date(r.created)>='{start_date}' AND to_date(r.created) <='{end_date}'
          AND r.commission_amount_euro <= {max_rpb}
          AND r.status not in ('fraudulent', 'test', 'unknown')

        """.format(start_date = self.start_date, end_date = self.end_date,
                   max_rpb = self.max_rpb, account_2_partner_id = self.partner_id,
                   reservation_table = self.reservation_table,
                   affiliate_table = self.affiliate_table)

        reservations = spark.sql(reservation_query)

         # This is the preferred source for NITS
        nits_scores_query = """
        SELECT hotelreservation_id
            , prediction nits_pred
            FROM {nits_score_table}
            WHERE created_date >= '{start_date}' and
            created_date <= '{end_date}'
        """.format(start_date = self.start_date, end_date = self.end_date,
                   nits_score_table = self.nits_score_table)

        # the hotelreservation_id is a long while in the other
        # dataframes is a string we convert
        nits_scores = spark.sql(nits_scores_query)\
                           .withColumn("hotelreservation_id_string",
                                       f.col("hotelreservation_id").cast(t.StringType()))\
                           .drop("hotelreservation_id")\
                           .withColumnRenamed("hotelreservation_id_string",
                                              "hotelreservation_id")

        # This is the postbook (less reliable) NITS
        postbook_nits_query = """
        SELECT hotelreservation_id
        , nits_score
        FROM {nits_postbook_table}
        WHERE  to_date(mysql_row_created_at)>='{start_date}' AND to_date(mysql_row_created_at) <='{end_date}'
        """.format(start_date = self.start_date, end_date = self.end_date,
                   nits_postbook_table = self.nits_postbook_table)

        nits_postbook = spark.sql(postbook_nits_query)

        revenues_agg_filtered = reservations\
                        .join(nits_scores,on = "hotelreservation_id", how="left_outer")\
                        .join(nits_postbook,on = "hotelreservation_id", how="left_outer")\
                        .selectExpr("cast(hotel_id as integer) as hotel_id",
                            "yyyy_mm_dd","locale","device",
                            "bookings",
                            "commission",
                            "roomnights",
                            "bookings   * (1-coalesce(nits_pred,nits_score,0.0)) as nits_bookings",
                            "commission * (1-coalesce(nits_pred,nits_score,0.0)) as nits_commission"
                            )\
                        .groupBy("hotel_id","locale","device","yyyy_mm_dd")\
                        .agg(f.sum(f.col("bookings")).alias("bookings"),
                             f.sum(f.col("commission")).alias("commission"),
                             f.sum(f.col("nits_bookings")).alias("nits_bookings"),
                             f.sum(f.col("nits_commission")).alias("nits_commission"),
                             f.sum(f.col("roomnights")).alias("roomnights"))

        perf_table = clicks\
                        .join(revenues_agg_filtered,
                        on = ["hotel_id","locale","device","yyyy_mm_dd"],
                        how = "outer")\
                        .groupBy(*self.agg_on)\
                        .agg(f.sum("nits_bookings").alias("nits_bookings")
                            ,f.sum("nits_commission").alias("nits_commission")
                            ,f.sum("bookings").alias("gross_bookings")
                            ,f.sum("commission").alias("gross_commission")
                            ,f.sum("cost").alias("cost")
                            ,f.sum("clicks").alias("clicks")
                            ,f.sum("roomnights").alias("roomnights"))\
                        .withColumn("nits_profit",f.expr("nits_commission-cost"))\
                        .withColumn("gross_profit", f.expr("gross_commission-cost"))

        return (perf_table)

    def get_cancellations(self):
        """function to obtain cancellation (cancelled bookings and cancelled commissions)
        stats for account_2 at agg_on level

        Returns:
            spark dataframe of number of cancellations, cancelled cancelled_roomnights
            and also cancelled commissions at aggregated dimensions
        """

        cancellations_query = """
        SELECT r.date_cancelled AS yyyy_mm_dd,
             r.affiliate_id,
             (case when length(regexp_extract(r.label, '[-]core(.*)[-]hotel', 1)) > 0
                    then upper(regexp_extract(r.label, '[-]core(.*)[-]hotel', 1))
                    else 'US'
                    end) locale,
             (case when r.label rlike '(linkmob[-]core.*[-]hotel)' then 'mobile'
                   when a.affiliate_name rlike '(Mobile)' then 'mobile'
                   else 'web'
                   end) device,
             SUM(1) AS cancellations,
             SUM(r.commission_amount_euro) AS comm_canc_asbooked,
             SUM(roomnights) AS roomnights
        FROM {reservation_table} r
        JOIN (
        SELECT id affiliate_id
               , name affiliate_name
          FROM {affiliate_table}
         WHERE partner_id = {account_2_partner_id}
           AND name rlike '([_][Cc]ore[.])'
           ) a
        ON r.affiliate_id = a.affiliate_id
        WHERE  date_cancelled >= '{start_date}'
        AND  date_cancelled <= '{end_date}'
        AND  status not in ('fraudulent', 'test', 'unknown')
        GROUP BY r.date_cancelled,
             r.affiliate_id,
             (case when length(regexp_extract(r.label, '[-]core(.*)[-]hotel', 1)) > 0
                    then upper(regexp_extract(r.label, '[-]core(.*)[-]hotel', 1))
                    else 'US'
                    end),
             (case when r.label rlike '(linkmob[-]core.*[-]hotel)' then 'mobile'
                   when a.affiliate_name rlike '(Mobile)' then 'mobile'
                   else 'web'
                   end)
        """.format(reservation_table = self.reservation_table,
                   affiliate_table = self.affiliate_table,
                   account_2_partner_id = self.partner_id,
                   start_date = self.start_date,
                   end_date = self.end_date)

        cancellations = spark.sql(cancellations_query)

        cancellations_agg = cancellations.groupBy(*self.agg_on)\
                     .agg(f.sum("cancellations").alias("cancellations"),
                          f.sum("comm_canc_asbooked").alias("cancelled_commission"),
                          f.sum("roomnights").alias("cancelled_roomnights"))

        return (cancellations_agg)

    def get_agg_stats(self):
        """function that join cancellations data with performance metrics at
        desired aggregated dimensions, the current aggregated dimension remove the
        hotel_id, this can be easily adapted by just group by self.agg_on

        Returns:
            spark dataframe
        """
        cancellations_agg = self.get_cancellations()

        perf_table = self.get_stats_summary()

        agg_perf_table = perf_table.join(cancellations_agg, how = "outer", on = self.agg_on)\
                               .withColumn("asbooked_commission",f.expr("gross_commission-cancelled_commission"))\
                               .withColumn("asbooked_roomnights", f.expr("roomnights - cancelled_roomnights"))

        agg_perf_table = agg_perf_table.na.fill(0, subset = list(set(agg_perf_table.columns) - set(self.agg_on)))

        return (agg_perf_table)
