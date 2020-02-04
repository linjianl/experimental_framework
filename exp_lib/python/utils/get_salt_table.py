from pyspark.sql import SparkSession
import pandas as pd
spark = SparkSession.builder.getOrCreate()

def get_salt_table(base_salt, run_date, ntrials):
    """get a table with various salts with the number of salts equal to number of ntrials (iterations).

    Args:
        base_salt (string): the first character in a salt .
        run_date (string) : the date an experiment is run 
        ntrials (int)     : number of trials to be generated. 

    Returns:
        spark dataframe with a column of salts, maximum length of salts is 10 characters.  
    """

    salts = [base_salt + run_date[-5:].replace("-","")+str(i).zfill(len(str(ntrials)))
            for i in range(ntrials)]
    # only keep 10 digits for salt 
    salts = [salt[:10] for salt in salts]
    salt_table = spark.createDataFrame(pd.DataFrame(data=salts,columns=["salt"]))
    
    return salt_table 

