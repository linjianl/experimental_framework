from __future__ import division, print_function
import pandas as pd
import numpy as np
from math import ceil
import logging
from statsmodels.tsa.stattools import adfuller, kpss
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession, Window, Row

from exp_lib.utils.randomization import assign_group
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

def get_perf_metrics_stats(Metrics, metric_names):
    """ get the metric for today-(period+dminus) to (today-dminus)
        dminus is here to take out recent data if we still have not
        received the cost report
        
        Args:
            Metrics:
            metric_names: list of strings
            
        Returns:
            broadcasted variable    
    """
    
    for i, Metric in enumerate(Metrics):
        if  i == 0:
            inv_w_metrics = Metric.values
        else:
            inv_w_metrics = inv_w_metrics.join(Metric.values,on=["hotel_id","yyyy_mm_dd"],how="outer")

    select_criteria = [m+"!=0" for m in metric_names]
    summary = ( inv_w_metrics.fillna({m:0 for m in metric_names})
                  .where(" or ".join(select_criteria)).cache() )
    logging.info("Number of row in metrics table: {}".format(summary.count()))
    
    # broadcasting the metrics to all executors
    df_summary = summary.toPandas()
    bc_summary = sc.broadcast(df_summary)
    
    return bc_summary 


def create_loss_udf(metric_names,perf,control,test,weights,exp_groups,period):
    """ udf to calculate the relative difference for the test against control groups for given performance metrics
    
    Args:
        metric_names: list, metric names 
        perf        : values from broadcasted variable 
        salt        : string, randomization 
        control     : list or integer, which group is control group
        test        : list or integer, which group is control group
        weights     : list, minimize weight per group 
        exp_groups  : list, how much weight each exp_groups represent 
        period      : integer, number of days in the pre-experiment period
    
    Returns:
        a spark udf. This udf will only be called in the base.py script.
        Note that the spark udf has to be returned in the function and can not be registered outside of the functions. 
        For a detailed explanation, see 
        https://stackoverflow.com/questions/35923775/functions-from-custom-module-not-working-in-pyspark-but-they-work-when-inputted#_=_
    """
    
    def loss_func(salt):
        temp_ = perf.copy()
        temp_["group"] = temp_["hotel_id"].map(lambda x: assign_group(x, salt, weights))
        
        exp_group_lookup = []
        for i,n in enumerate(exp_groups):
            exp_group_lookup += [i]*n
        temp_["exp_group"] = temp_["group"].map(lambda i: exp_group_lookup[i])

        # getting the number of properties per experiment group
        htls_per_exp_group = ( temp_.groupby(["exp_group","hotel_id"],as_index=False)
                                .count().groupby("exp_group",as_index=False)["hotel_id"].count() )
        htls_per_exp_group.columns = ["exp_group","num_hotels"]

        daily_grouped_perf = ( temp_.groupby(["exp_group","yyyy_mm_dd"],as_index=False)
                                .agg({m:"sum" for m in metric_names})
                                .merge(htls_per_exp_group,on="exp_group",how="inner") )

        for m in metric_names:
            daily_grouped_perf["avg_"+m] = daily_grouped_perf[m]/daily_grouped_perf["num_hotels"]

        daily_perf_control =  ( daily_grouped_perf
                                 .loc[(daily_grouped_perf.exp_group.isin(control)),
                                      ["yyyy_mm_dd"]+["avg_"+m for m in metric_names]]
                                 .copy() )
        daily_perf_control.columns = ["yyyy_mm_dd"]+["control_"+m for m in metric_names]

        daily_perf_variant =  ( daily_grouped_perf
                                 .loc[(daily_grouped_perf.exp_group.isin(test)),
                                      ["yyyy_mm_dd","exp_group"]+["avg_"+m for m in metric_names]]
                                 .copy() )

        control_vs_variant = daily_perf_control.merge(daily_perf_variant,on="yyyy_mm_dd",how="left")

        # return absolute relative difference
        for m in metric_names:
            control_vs_variant["pae_"+m] = abs(abs(control_vs_variant["avg_"+m]/control_vs_variant["control_"+m]) - 1)

        total_pae = np.sum([np.sum(control_vs_variant["pae_"+m]) for m in metric_names])
        pae = float(total_pae/period/len(exp_groups))
        return pae
    
    return f.udf(loss_func, t.FloatType())

def create_stationarity_udf(metric_names, control, test, perf, exp_groups, weights):
    """ udf to conduct the stationarity tests between the test against control groups for given performance metrics
    
    Args:
        metric_names: list, metric names 
        perf        : values from broadcasted variable 
        salt        : string, randomization 
        control     : list or integer, which group is control group
        test        : list or integer, which group is control group
        weights     : list, minimize weight per group 
        exp_groups  : list, how much weight each exp_groups represent 
    
    Returns:
        a spark udf. This udf will only be called in the base.py script.
        Note that the spark udf has to be returned in the function and can not be registered outside of the functions. 
        For a detailed explanation, see 
        https://stackoverflow.com/questions/35923775/functions-from-custom-module-not-working-in-pyspark-but-they-work-when-inputted#_=_
    """
    
    def stationarity_func(salt):
        
        # generate experiment groups that each hotel_id belong to
        temp_ = perf.copy()
        temp_["group"] = temp_["hotel_id"].map(lambda x: assign_group(x,salt,weights))
        exp_group_lookup = []
        for i,n in enumerate(exp_groups):
            exp_group_lookup += [i]*n
        temp_["exp_group"] = temp_["group"].map(lambda i: exp_group_lookup[i])

        # getting the daily performance at experiment group level
        daily_exp_group_perf = ( temp_.groupby(["yyyy_mm_dd", "exp_group"],as_index=False)
                                .agg({m:"sum" for m in metric_names}) )
        # separate test and control groups
        daily_exp_group_perf_control =  ( daily_exp_group_perf
                                 .loc[(daily_exp_group_perf.exp_group.isin(control)),
                                      ["yyyy_mm_dd"]+[m for m in metric_names]]
                                 .copy() )
        daily_exp_group_perf_control.columns = ["yyyy_mm_dd"]+["control_"+m for m in metric_names]

        daily_exp_group_perf_variant =  ( daily_exp_group_perf
                                 .loc[(daily_exp_group_perf.exp_group.isin(test)),
                                      ["yyyy_mm_dd","exp_group"]+[m for m in metric_names]]
                                 .copy() )

        control_vs_variant = daily_exp_group_perf_control.merge(daily_exp_group_perf_variant,
                            on = "yyyy_mm_dd",how = "left")

        for m in metric_names:
            control_vs_variant["stat_"+m] = control_vs_variant[m]/control_vs_variant["control_"+m]

        # calculate test_stats_result
        stat_test_result = []
        for m in metric_names:
            for i in control_vs_variant.exp_group.unique():
                stat_test_result.append(adfuller(control_vs_variant.loc[control_vs_variant.exp_group == i]["stat_" + m])[1] < 0.05)
                stat_test_result.append(kpss(control_vs_variant.loc[control_vs_variant.exp_group == i]["stat_" + m])[1] > 0.05)

        return (all(stat for stat in stat_test_result))
    return f.udf(stationarity_func, t.BooleanType())

def create_bootstrapped_udf(metric_names,perf,period,weights,exp_groups_list = [[1]*20, [2]*10, [5]*4, [10]*2]):
    """ udf to conduct the bootstrapped results for A/A testing for any random 
    
    Args:
        metric_names: list, metric names 
        perf        : performance table
        salt        : string, randomization 
        control     : list or integer, which group is control group
        test        : list or integer, which group is control group
        weights     : list, minimize weight per group, default to [1] * 20, to divide to smallest weight to 5% each.  
        exp_groups_list: list, how much weight each exp_groups represent, 
                         default to [[1]*20, [2]*10, [5]*4, [10]*2] to test for 5%, 10%, 25% and 50%. 
    
    Returns:
        a spark udf. This udf will only be called in the base.py script.
        Note that the spark udf has to be returned in the function and can not be registered outside of the functions. 
        For a detailed explanation, see 
        https://stackoverflow.com/questions/35923775/functions-from-custom-module-not-working-in-pyspark-but-they-work-when-inputted#_=_
    """
    
    def bootstrapped_func(salt):
        temp_ = perf.copy()
        temp_["group"] = temp_["hotel_id"].map(lambda x: assign_group(x, salt, weights))
        days_result = pd.DataFrame()

        for iter_groups in exp_groups_list:
            exp_group_lookup = []
            for i,n in enumerate(iter_groups):
                exp_group_lookup += [i]*n
            temp_["exp_group"] = temp_["group"].map(lambda i: exp_group_lookup[i])

            daily_exp_group_perf = ( temp_.groupby(["yyyy_mm_dd", "exp_group"],as_index=False)
                                            .agg({m:"sum" for m in metric_names}) )

            new_index = list(daily_exp_group_perf.columns)
            daily_exp_group_perf['paired'] = daily_exp_group_perf['exp_group'].map(lambda x: range(x+1, max(exp_group_lookup)+1))
            daily_exp_group_perf_paired = ( daily_exp_group_perf
                                    .set_index(new_index)
                                    .paired.apply(pd.Series)
                                    .stack()
                                    .to_frame('paired')
                                    .reset_index() )

            daily_exp_group_perf_paired['paired'] = daily_exp_group_perf_paired['paired'].map(lambda x: int(x))
            paired_perf = pd.merge(daily_exp_group_perf_paired,
                                   (daily_exp_group_perf
                                    .drop(columns = 'paired')
                                    .rename(columns = {'exp_group':'paired'})),
                               left_on = ['yyyy_mm_dd', 'paired'],
                               right_on = ['yyyy_mm_dd', 'paired'],
                               how = 'left')
            for m in metric_names:
                paired_perf["rdiff_"+m] = paired_perf[m + "_x"]/ paired_perf[m + "_y"] - 1

            paired_perf['paired_groups'] = paired_perf['exp_group'].astype(str) + '_' +paired_perf['paired'].astype(str)


            weekdays = 7
            num_weeks = int(period/weekdays)
            for i in range(1, num_weeks + 1):
                num_days = i * weekdays
                result = ( paired_perf[paired_perf.yyyy_mm_dd.isin(paired_perf.yyyy_mm_dd.unique()[:num_days])]
                                                                   .groupby("paired_groups",as_index=False)
                                            .agg({"rdiff_" + m:"mean" for m in metric_names}) )
                result['num_days']   = num_days
                result['num_groups'] = max(exp_group_lookup) + 1
                days_result =  days_result.append(result)

        return  list(days_result.itertuples(index=False, name = 'None'))
    
    bootstrapped_arary_returnType =  t.ArrayType(t.StructType([t.StructField('paired_groups', t.StringType(), nullable=False),            
                             t.StructField('rdiff_metric_1', t.FloatType(), nullable=False),
                             t.StructField('rdiff_metric_2', t.FloatType(), nullable=False),
                             t.StructField('num_days', t.IntegerType(), nullable=False),
                             t.StructField('num_groups', t.IntegerType(), nullable=False)])) 
    
    return f.udf(bootstrapped_func, bootstrapped_arary_returnType)

def create_loss_udf_v2(metric_names,perf,control,test,weights,exp_groups,period):
    """ udf to calculate the relative difference for the test against control groups for given performance metrics
    
    Args:
        metric_names: list, metric names 
        perf        : values from broadcasted variable 
        salt        : string, randomization 
        control     : list or integer, which group is control group
        test        : list or integer, which group is control group
        weights     : list, minimize weight per group 
        exp_groups  : list, how much weight each exp_groups represent 
        period      : integer, number of days in the pre-experiment period
    
    Returns:
        a spark udf. This udf will only be called in the base.py script.
        Note that the spark udf has to be returned in the function and can not be registered outside of the functions. 
        For a detailed explanation, see 
        https://stackoverflow.com/questions/35923775/functions-from-custom-module-not-working-in-pyspark-but-they-work-when-inputted#_=_
    """
    
    def loss_func(salt):
        temp_ = perf.copy()
        temp_["group"] = temp_["hotel_id"].map(lambda x: assign_group(x, salt, weights))
        
        exp_group_lookup = []
        for i,n in enumerate(exp_groups):
            exp_group_lookup += [i]*n
        temp_["exp_group"] = temp_["group"].map(lambda i: exp_group_lookup[i])

        # getting the number of properties per experiment group
        htls_per_exp_group = ( temp_.groupby(["exp_group","hotel_id"],as_index=False)
                                .count().groupby("exp_group",as_index=False)["hotel_id"].count() )
        htls_per_exp_group.columns = ["exp_group","num_hotels"]

        daily_grouped_perf = ( temp_.groupby(["exp_group","yyyy_mm_dd"],as_index=False)
                                .agg({m:"sum" for m in metric_names})
                                .merge(htls_per_exp_group,on="exp_group",how="inner") )

        for m in metric_names:
            daily_grouped_perf["avg_"+m] = daily_grouped_perf[m]/daily_grouped_perf["num_hotels"]

        daily_perf_control =  ( daily_grouped_perf
                                 .loc[(daily_grouped_perf.exp_group.isin(control)),
                                      ["yyyy_mm_dd"]+["avg_"+m for m in metric_names]]
                                 .copy() )
        daily_perf_control.columns = ["yyyy_mm_dd"]+["control_"+m for m in metric_names]

        daily_perf_variant =  ( daily_grouped_perf
                                 .loc[(daily_grouped_perf.exp_group.isin(test)),
                                      ["yyyy_mm_dd","exp_group"]+["avg_"+m for m in metric_names]]
                                 .copy() )

        control_vs_variant = daily_perf_control.merge(daily_perf_variant,on="yyyy_mm_dd",how="left")

        # return absolute relative difference
        for m in metric_names:
            control_vs_variant["pae_"+m] = abs(abs(control_vs_variant["avg_"+m]/control_vs_variant["control_"+m]) - 1)

        total_pae = np.sum([np.sum(control_vs_variant["pae_"+m]) for m in metric_names])
        pae = float(total_pae/period)
        return pae
    
    return f.udf(loss_func, t.FloatType())

def create_loss_udf_v3(metric_names,perf,control,test,weights,exp_groups,period):
    """ udf to calculate the relative difference for the test against control groups for given performance metrics (inverse variance weighted)
    
    Args:
        metric_names: list, metric names 
        perf        : values from broadcasted variable 
        salt        : string, randomization 
        control     : list or integer, which group is control group
        test        : list or integer, which group is control group
        weights     : list, minimize weight per group 
        exp_groups  : list, how much weight each exp_groups represent 
        period      : integer, number of days in the pre-experiment period
    
    Returns:
        a spark udf. This udf will only be called in the base.py script.
        Note that the spark udf has to be returned in the function and can not be registered outside of the functions. 
        For a detailed explanation, see 
        https://stackoverflow.com/questions/35923775/functions-from-custom-module-not-working-in-pyspark-but-they-work-when-inputted#_=_
    """
    
    def loss_func(salt):
        temp_ = perf.copy()
        temp_["group"] = temp_["hotel_id"].map(lambda x: assign_group(x, salt, weights))
        
        exp_group_lookup = []
        for i,n in enumerate(exp_groups):
            exp_group_lookup += [i]*n
        temp_["exp_group"] = temp_["group"].map(lambda i: exp_group_lookup[i])

        # getting the number of properties per experiment group
        htls_per_exp_group = ( temp_.groupby(["exp_group","hotel_id"],as_index=False)
                                .count().groupby("exp_group",as_index=False)["hotel_id"].count() )
        htls_per_exp_group.columns = ["exp_group","num_hotels"]

        daily_grouped_perf = ( temp_.groupby(["exp_group","yyyy_mm_dd"],as_index=False)
                                .agg({m:"sum" for m in metric_names})
                                .merge(htls_per_exp_group,on="exp_group",how="inner") )

        for m in metric_names:
            daily_grouped_perf["avg_"+m] = daily_grouped_perf[m]/daily_grouped_perf["num_hotels"]

        daily_perf_control =  ( daily_grouped_perf
                                 .loc[(daily_grouped_perf.exp_group.isin(control)),
                                      ["yyyy_mm_dd"]+["avg_"+m for m in metric_names]]
                                 .copy() )
        daily_perf_control.columns = ["yyyy_mm_dd"]+["control_"+m for m in metric_names]

        daily_perf_variant =  ( daily_grouped_perf
                                 .loc[(daily_grouped_perf.exp_group.isin(test)),
                                      ["yyyy_mm_dd","exp_group"]+["avg_"+m for m in metric_names]]
                                 .copy() )

        control_vs_variant = daily_perf_control.merge(daily_perf_variant,on="yyyy_mm_dd",how="left")

        # relative difference
        for m in metric_names:
            control_vs_variant["pae_"+m] = control_vs_variant["avg_"+m]/control_vs_variant["control_"+m] - 1
        
        print(control_vs_variant)
        
        pae_list = [np.sum(abs(control_vs_variant["pae_"+m]))/np.var(control_vs_variant["pae_"+m]) 
                            for m in metric_names]
        print(pae_list)
        
        total_pae = np.sum(pae_list)
        
        pae = float(total_pae/period)
        
        return pae
    
    return f.udf(loss_func, t.FloatType())