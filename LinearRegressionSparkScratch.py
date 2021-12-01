import sys
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, functions as F , Row

def matrix_multiply(row):
  row = row.asDict()
  for i in independent_columns:
    (ki, vi) = (i, row[i])
    if(calculate == 'xt.x'):
        for j in independent_columns:
            (kj, vj) = (j, row[j])
            yield ((ki,kj), vi * vj)
    else:
        yield (ki, vi * row[dependent_column])


if __name__ == "__main__":
  
  if len(sys.argv) != 2:
    print("Usage: spark-submit Assignment2.py <inputfilename.csv>", file=sys.stderr)
    sys.exit(-1)
  
  spark = (SparkSession.builder.appName("LinearRegression").getOrCreate())
  sc = spark.sparkContext
  sc.setLogLevel('WARN')
  
  file_name = sys.argv[1]
  
  #Read file and cast columns to double data type
  df = spark.read.format("csv").load(file_name)
  df = df.select([F.col(c).cast("double") for c in df.columns])
  
  #Get column names of dependent and independent variables
  independent_columns = df.columns[:-1]
  dependent_column = df.columns[-1]
  
  #Add column with all value 1 to df and save variable name to start of dependent variable list
  df = df.withColumn('one', F.lit(1))
  independent_columns.insert(0,'one')
  
  #Start calculation of X.T * X
  calculate = 'xt.x'
  xtx_data = ( df.rdd
    .flatMap(matrix_multiply)
    .reduceByKey(lambda a, b: a + b)
    .collect()
  )
  
  #Start calculation of X.T * Y
  calculate = 'xt.y'
  xty_data = ( df.rdd
    .flatMap(matrix_multiply)
    .reduceByKey(lambda a, b: a + b)
    .collect()
  )
  
  #Create px1 matrix of X.T * Y
  xty = np.array([[x] for i,x in xty_data])
  
  #Create pxp matrix of X.T * X
  lst = []
  lst_inner = []
  for i in range(0,len(xtx_data)):
      if(len(lst_inner) < len(independent_columns)):
          lst_inner.append(xtx_data[i][1])
          
      else:
          lst.append(lst_inner)
          lst_inner = [xtx_data[i][1]]
  lst.append(lst_inner)
  xtx = np.array(lst)
  
  #Calculate Inverse of X.T * X
  xtx_inv = np.linalg.inv(xtx)
  
  
  beta_matrix = np.dot(xtx_inv,xty)
  np.set_printoptions(suppress=True)
  print(beta_matrix)