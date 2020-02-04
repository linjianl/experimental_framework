from __future__ import division, print_function
import logging
import os
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window
import string
import pandas as pd

spark = ( SparkSession
             .builder
             .getOrCreate() )

sc = spark.sparkContext
from exp_lib.account.base import ExperimentBase, MetricBase

class account(ExperimentBase):
    """
    Tripadivosr account class, child class of experimentBase class, the child class specifies the GHA account specific exp_weight,
    we do not set in advance because it is dynamic. Thus, every attribute is inherited from the experimentBase parent class.
    """
    pass

class NitsBookings(MetricBase):
    """
    NitsBookings (per property) class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account
    
    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : string, point of sale, booker_country 
        use_cuped   : boolean, use CUPED method or not 
        cuped_period: integer, period for CUPED 
    """


    def __init__(self,start_date,end_date,pos,use_cuped=False,cuped_period=None):
  
        self.metric_name = "nits_bookings"
        super(NitsBookings,self).__init__(self.metric_name,start_date,end_date,pos,use_cuped,cuped_period)

    def compute(self):

        tripadvisorStats = TripAdvisorStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( tripadvisorStats.get_stats_summary()
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
        super(NitsProfit,self).__init__(self.metric_name,start_date,end_date,pos,use_cuped,cuped_period)

    def compute(self):

        tripadvisorStats = TripAdvisorStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( tripadvisorStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )

class GrossBookings(MetricBase):
    
    """
    GrossBookings (per Property) class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account
    
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

        tripadvisorStats = TripAdvisorStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( tripadvisorStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )


class GrossProfit(MetricBase):
    """
    GrossProfit (per Property) class, child class of MetricBase class, the child class obtain the defined metrics for the relevant account
    
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

        tripadvisorStats = TripAdvisorStats(start_date=self.start_date,end_date=self.end_date,pos=self.pos)
        return ( tripadvisorStats.get_stats_summary()
                   .select("hotel_id","yyyy_mm_dd",self.metric_name) )

class TripAdvisorStats(object):
    """
    TripAdvisorStats class, obtain data for relevant performance metrics

    Attributes:
        start_date  : string, the start date to obtain performance
        end_date    : string, the end date to obtain performance
        pos         : list or string, point of sale, booker_country, default to 'All'
        max_rpb     : float, max revenue per booking as cutoff point to remove bookings with uncommon values 
                      (normal or fraudulent) for smart salt calculation, this is not necessary when calculating 
                      for daily performance
        agg_on      : dimension to aggregate on 
        partner_id  : integer, relevant partner id for the specific account
        costs_table : string, relevant cost table for the specific account 
        nits_score_table: string, more reliable nits score table 
        nits_postbook_table: string, postbook (less reliable) NITS table 
        reservation_table: string, best source of reservation table 
        affiliate_table: string, affiliate table which is joined with reservation table on afilliate id        
    """

    def __init__(self,start_date,end_date,pos=['All'],max_rpb=3000.0,partner_id=404815,
                agg_on = ["hotel_id", "yyyy_mm_dd"],
                costs_table = 'spmeta.trip_clickcost',
                reservation_table = 'default.dw_reservation',
                nits_score_table = 'nits.scores',
                nits_postbook_table = 'default.reslog_nits_ng_postbooking_score',
                affiliate_table = 'default.bp_b_affiliate',
                placements =['DMeta', 'Mobile']):

        self.start_date = start_date
        self.end_date   = end_date
        self.pos        = pos if isinstance(pos, list) else [pos]
        self.max_rpb    = max_rpb
        self.partner_id = partner_id
        self.agg_on     = agg_on
        self.reservation_table = reservation_table
        self.nits_score_table = nits_score_table
        self.nits_postbook_table = nits_postbook_table
        self.affiliate_table = affiliate_table
        self.costs_table = costs_table
        self.placements = placements  # temporarily set the device as such, now not allow for placements filter 

    def get_stats_summary(self):
        """function to get performance summary stats at desired aggregated level.

        Returns:
            spark dataframe with metrics such as yyyy_mm_dd, hotel_id, 
            nits_bookings,gross_bookings,nits_profit,gross_profit
        """

        # Note that the code is adapted from tripadvisor bidding production code
        # https://gitlab.booking.com/ShopPPC/tripadvisor-bidding-production/blob/master/pyspark-
        # scripts/bidding/cpc_plain_bidder.py
        # we take TripAd reservations;
        # we use last click attribution so the hotel HAS to be taken
        # from the label
        # we use the affiliate_id lookup table to find TripAd
        # we take hotel ufi from reservation
        
        reservation_query = string.Template(
            """
            SELECT
            r.id hotelreservation_id
                , coalesce(cast(regexp_extract(r.label, 'hotel[-](.*)[_]xqdz', 1) as int)
                           , r.hotel_id) hotel_id
                , regexp_extract(a.affiliate_name, '^(.*)[.](.*)[.].*', 1) distribution
                , regexp_extract(a.affiliate_name, '^(.*)[.](.*)[.].*', 2) placement
                , 1 bookings
                , r.commission_amount_euro commission
                , cast(to_date(r.created) as string) as yyyy_mm_dd
                , r.hotel_ufi as hotel_ufi_reservation
                , roomnights
                FROM ${reservation_table} r
                JOIN
                (
                SELECT id affiliate_id
                , name affiliate_name
                FROM ${affiliate_table}
                WHERE partner_id = ${tripad_partner_id}
                and name rlike '(${placement1}|${placement2})'
                ) a
                ON r.affiliate_id = a.affiliate_id
                WHERE to_date(r.created) >= '${start_date}' and to_date(r.created) <= '${end_date}'
                and r.commission_amount_euro < ${commission_cap}
                and r.status not in ('fraudulent', 'test', 'unknown')
            """)\
            .substitute({
                "commission_cap" : self.max_rpb,
                "placement1" : self.placements[0],
                "placement2" : self.placements[1],
                "end_date" : self.end_date,
                "start_date" : self.start_date,
                "tripad_partner_id" : self.partner_id,
                "reservation_table": self.reservation_table,
                "affiliate_table": self.affiliate_table
                })

        reservations = spark.sql(reservation_query)

        # This is the preferred source for NITS
        nits_scores_query = string.Template(
            """
            SELECT hotelreservation_id
                , prediction nits_pred
                FROM ${nits_score_table}
                WHERE created_date >= '${start_date}' and
                created_date <= '${end_date}'
            """)\
            .substitute({
                "end_date" : self.end_date,
                "start_date" : self.start_date,
                "nits_score_table": self.nits_score_table
                })

        # the hotelreservation_id is a long while in the other
        # dataframes is a string we convert
        nits_scores = spark.sql(nits_scores_query)\
                           .withColumn("hotelreservation_id_string",
                                       f.col("hotelreservation_id").cast(t.StringType()))\
                           .drop("hotelreservation_id")\
                           .withColumnRenamed("hotelreservation_id_string",
                                              "hotelreservation_id")

        # This is the postbook (less reliable) NITS
        postbook_nits_query = string.Template(
              """
               SELECT hotelreservation_id
               , nits_score
               FROM ${nits_postbook_table}
               WHERE to_date(mysql_row_created_at) >=
              '${start_date}' and
              to_date(mysql_row_created_at) <= '${end_date}'
              """)\
              .substitute({
                  "nits_postbook_table" : self.nits_postbook_table,
                  "end_date" : self.end_date,
                  "start_date" : self.start_date
                  })

        nits_postbook = spark.sql(postbook_nits_query)

        # regular expressions for getting distribution,
        # placement, etc...
        query_regexs = {}
        query_regexs["dist_&_placement"] = string.Template(
           "(.*)(${placement1}|${placement2})")\
           .substitute({"placement1" : self.placements[0],
                        "placement2" : self.placements[1]}
                       )

        query_regexs["silo_filter"] = string.Template(
           "(${placement1}|${placement2})")\
           .substitute({"placement1" : self.placements[0],
                        "placement2" : self.placements[1]}
                       )

        # cost data
        click_cost_raw = spark.table(self.costs_table)\
            .filter(f.col("date") <= self.end_date)\
            .filter(f.col("date") >= self.start_date)\
            .filter(~f.isnull(f.col("hour")))\
            .filter(~f.isnull(f.regexp_extract(f.col("siloname"),
                                                     query_regexs["silo_filter"],1)))\
                        .selectExpr("partnerpropertyid as hotel_id",
                                    "siloname", "1 as clicks", "cost_eur as cost",
                                    "length_of_stay",
                                    "cast(date as string) yyyy_mm_dd")\
            .withColumn("distribution", f.regexp_extract(f.col("siloname"),
                                                            query_regexs["dist_&_placement"], 1))\
            .withColumn("placement", f.regexp_extract(f.col("siloname"),
                                                            query_regexs["dist_&_placement"], 2))

        revenues_agg_filtered = reservations\
                                .join(nits_scores,
                                    on = "hotelreservation_id", how="left_outer")\
                                .join(nits_postbook,
                                    on = "hotelreservation_id", how="left_outer")\
                                .selectExpr("cast(hotel_id as integer) as hotel_id",
                                            "hotel_ufi_reservation",
                                    "distribution", "placement", "yyyy_mm_dd",
                                    "bookings",
                                    "commission",
                                    "roomnights",
                                    "bookings   * (1-coalesce(nits_pred,nits_score,0.0)) as nits_bookings",
                                    "commission * (1-coalesce(nits_pred,nits_score,0.0)) as nits_commission"
                                    )\
                        .groupBy("hotel_id","distribution","placement","yyyy_mm_dd")\
                        .agg(f.sum(f.col("bookings")).alias("bookings"),
                             f.sum(f.col("commission")).alias("commission"),
                             f.sum(f.col("nits_bookings")).alias("nits_bookings"),
                             f.sum(f.col("nits_commission")).alias("nits_commission"),
                             f.sum(f.col("roomnights")).alias("roomnights"))

        click_cost_agg_filtered = click_cost_raw\
                      .groupBy("hotel_id","distribution","placement","yyyy_mm_dd")\
                      .agg(f.sum(f.col("clicks")).alias("clicks")
                          ,f.sum(f.col("cost")).alias("cost")
                          ,f.sum(f.col("length_of_stay")).alias("length_of_stay"))

        perf_table = click_cost_agg_filtered.join(revenues_agg_filtered,
                                on = ["hotel_id", "distribution", "placement","yyyy_mm_dd"],
                                how = "outer")
        
        if self.pos == ['All']:
            perf_table = perf_table\
                    .groupBy(*self.agg_on)\
                    .agg(f.sum("nits_bookings").alias("nits_bookings")
                         ,f.sum("nits_commission").alias("nits_commission")
                         ,f.sum("bookings").alias("gross_bookings")
                         ,f.sum("commission").alias("gross_commission")
                         ,f.sum("cost").alias("cost")
                         ,f.sum("clicks").alias("clicks")
                         ,f.sum("length_of_stay").alias("length_of_stay")
                         ,f.sum("roomnights").alias("roomnights"))\
                   .withColumn("nits_profit",f.expr("nits_commission-cost"))\
                   .withColumn("gross_profit", f.expr("gross_commission-cost"))

            return (perf_table)

        else:
            filtered_pos = spark.createDataFrame(pd.DataFrame(data = self.pos, 
                                                              columns = ["distribution"]))
            perf_table = perf_table\
                    .join(filtered_pos, on = "distribution", how = "inner")\
                    .groupBy(*self.agg_on)\
                    .agg(f.sum("nits_bookings").alias("nits_bookings")
                         ,f.sum("nits_commission").alias("nits_commission")
                         ,f.sum("bookings").alias("gross_bookings")
                         ,f.sum("commission").alias("gross_commission")
                         ,f.sum("cost").alias("cost")
                         ,f.sum("clicks").alias("clicks")
                         ,f.sum("length_of_stay").alias("length_of_stay")
                         ,f.sum("roomnights").alias("roomnights"))\
                   .withColumn("nits_profit",f.expr("nits_commission-cost"))\
                   .withColumn("gross_profit", f.expr("gross_commission-cost"))

            return (perf_table)


    def get_cancellations(self):
        """function to obtain cancellation (cancelled bookings and cancelled commissions) stats.

        Returns: spark dataframe with cancelled commission, cancelled bookings and cancelled roomnights 
                at desired dimensions
        """

        cancellations_query = string.Template(
        """
        SELECT r.date_cancelled AS yyyy_mm_dd
               , r.hotel_id
               , regexp_extract(a.affiliate_name, '^(.*)[.](.*)[.].*', 1) distribution
               , regexp_extract(a.affiliate_name, '^(.*)[.](.*)[.].*', 2) placement
               , SUM(1) AS cancellations
               , SUM(r.commission_amount_euro) AS commission_cancelled
               , SUM(roomnights) AS roomnights
        FROM ${reservation_table} r
        JOIN (
        SELECT id affiliate_id
               , name affiliate_name
          FROM ${affiliate_table}
         WHERE partner_id = ${tripad_partner_id}
           AND name rlike '(${placement1}|${placement2})'
           ) a
        ON r.affiliate_id = a.affiliate_id
        WHERE  date_cancelled >= '${start_date}'
        AND  date_cancelled <= '${end_date}'
        AND  status not in ('fraudulent', 'test', 'unknown')
        GROUP BY r.date_cancelled
        , r.hotel_id
        , regexp_extract(a.affiliate_name, '^(.*)[.](.*)[.].*', 1)
        , regexp_extract(a.affiliate_name, '^(.*)[.](.*)[.].*', 2)
        """).substitute({
                "reservation_table" : self.reservation_table,
                "affiliate_table" : self.affiliate_table,
                "tripad_partner_id" : self.partner_id,
                "start_date" : self.start_date,
                "end_date" : self.end_date,
                "placement1" : self.placements[0],
                "placement2" : self.placements[1]})

        cancellations = spark.sql(cancellations_query)

        cancellations_agg = cancellations.groupBy(self.agg_on)\
                     .agg(f.sum("cancellations").alias("cancellations"),
                          f.sum("commission_cancelled").alias("cancelled_commission"),
                          f.sum("roomnights").alias("cancelled_roomnights"))
            
        if self.pos == ['All']:
            return (cancellations_agg)
        else:
            filtered_pos = spark.createDataFrame(pd.DataFrame(data = self.pos, 
                                                              columns = ["distribution"]))
            perf_table = cancellations_agg\
                    .join(filtered_pos, on = "distribution", how = "inner")
            return (cancellations_agg)
        
    def get_agg_stats(self):
        """function that join cancellations data with performance metrics at
        desired aggregated dimensions

        Returns:
            spark dataframe
        """

        cancellations_agg = self.get_cancellations()

        perf_table = self.get_stats_summary()

        agg_perf_table = perf_table.join(cancellations_agg, how = "outer", on = self.agg_on)\
                               .withColumn("asbooked_commission",f.expr("gross_commission-cancelled_commission"))\
                               .withColumn("asbooked_roomnights", f.expr("roomnights-cancelled_roomnights"))

        agg_perf_table = agg_perf_table.na.fill(0, subset = list(set(agg_perf_table.columns) - set(self.agg_on)))

        return (agg_perf_table)
