# Link Prediction

A Spark iplementation of three basic metrics for link prediction problem for social Networks:

* Common Neighbor
* Jaccard Coefficient
* Adamic/Adar

Each metric has its own method.

Given a file consist of connection such as:


file example
----------------
1&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2

1&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3

1&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4

7&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;2

7&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;3

7&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;5

7&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;11

7&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;13

2&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;4

2&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;11

11&nbsp;&nbsp;&nbsp;&nbsp;13

Numbers represent nodes (or users). Each line represent a connection. For example this represents connections 1-2, 1-3, 1-4 and so on. We want to measure how likely it is that user/node 1 will connect with user 7.

# Requirements

* Java - Maven
This is a Java Maven project so java and maven is required. All other dependencies included in pom.xml

* Hadoop - Spark
HDFS and Spark are required

# Run

	spark/bin/spark-submit --class org.spark.LinkPrediction pathToTarget/target/linkprediction-0.1.jar hdfs://localhost:54310/pathToInput/inputfile hdfs://localhost:54310/pathToOutput/output k method

* Assumed hdfs is in localhost.
* method options:
	* common
	* jaccard
	* adamicadar
* k:
	* k>0 for top-k
	* k==0 to get all new connections sorted
* input must be a .txt file in the form of the example above (or a .txt.gz file).
* if output dir is not empty, delete everything inside (or change output dir).
* if output dir does not exist it will automatically created.

### example

	$ spark/bin/spark-submit --class org.spark.LinkPrediction /vagrant/data/ergasia/linkprediction/target/linkprediction-0.1.jar hdfs://localhost:54310/linkprediction/input1 hdfs://localhost:54310/linkprediction/output 100 adamicadar
