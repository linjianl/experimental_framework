from __future__ import division, print_function
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import functions as f, types as t
spark = SparkSession.builder.getOrCreate()

def date_range(start_date,end_date):
    """ function to generate a list of date ranging from start to end date
    
    Args:
        start_date: string
        end_date  : string
        
    Returns: <list> date between start_date and end_date,
             including end_date
    """
    
    date_range = map(str,pd.date_range(start_date,end_date).date)
    
    return f.udf(date_range, returnType=t.ArrayType(t.StringType()))


def minusndays(date,n):
    """ function to generate minusndays
    
    Args:
        date: string, from which date to count backwards
        n   : integer, number of days to count backwards
    Return:
        string
    """
    
    date_format = "%Y-%m-%d"
    return (datetime.strptime(date,date_format) - timedelta(n)).strftime(date_format)