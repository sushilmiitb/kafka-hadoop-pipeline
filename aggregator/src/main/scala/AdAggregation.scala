/**
  * Created by rubbal on 4/2/17.
  */

import org.apache.spark.{SparkConf, SparkContext}

class AdAggregation {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    // get threshold
    val threshold = args(1).toInt

    // read in text file and split each document into words
    val metrics = sc.textFile(args(0)).flatMap(_.split(" "))

    metrics.foreach(x => println(x))

  }
}
