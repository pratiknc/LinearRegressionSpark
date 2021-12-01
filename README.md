Project Name: Linear Regression on Spark From Scratch
<br>LinearRegressionSparkScratch.py file implements Multiple Linear Regression using Ordinary Lease Squares Method on Spark Cluster. Matrix manipulation (Multiplication, Transpose, Inverse) are performed and Ordinary Least Squares are calculated.
<br>
<br>Copy the python file in a location on the Spark cluster at an accessible location.
<br>Place your input files to the Spark cluster and move them to HDFS using the below command<br>
	hdfs dfs -put [input file.csv] [HDFS Location]

<br>Input file assumptions:
<br>•	CSV file with multiple independent variables and one dependent variable is given as input.
<br>•	Dependent variable is the last column of the csv file.
<br>•	First line of csv corresponds to observation, i.e., first line is not a header.
<br>•	CSV file is a HDFS location with appropriate access permissions for the user running the code.
<br>
<br>To run the linear regression algorithm and display output on screen, execute the below command.
<br>spark-submit LinearRegressionSparkScratch.py <HDFS Location/input file.csv>
<br>
<br>To run the linear regression algorithm and write output to file, execute the below command.
<br>spark-submit Assignment2.py <HDFS Location/input file.csv> > <outfile_name>
