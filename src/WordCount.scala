import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
  * Created by jiajing on 15-12-8.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Word Count")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile("/usr/local/spark/spark-1.5.0-bin-hadoop2.6/README.md")
    val result = textRDD.flatMap(_.split("\\s+")).map((_, 1)).reduceByKey(_+_)
    result.foreach(println)
    sc.stop()
  }
}
