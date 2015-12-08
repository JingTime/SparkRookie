import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.math.Ordering

/**
  * Created by jiajing on 15-12-8.
  */
object TopK {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Word Count")
    val sc = new SparkContext(conf)
    val k = if (args.length > 0) args(0).toInt else 3
    val textRDD = sc.textFile("/usr/local/spark/spark-1.5.0-bin-hadoop2.6/README.md")
    val count = textRDD.flatMap(line => line.split("\\s+")).map(word => (word, 1)).reduceByKey(_+_)
    /*val sorted = count.map {
      case(key, value) => (value, key); //exchange key and value
    }.sortByKey(true, 1)
    val topK = sorted.top(k)*/
    val topK = count.top(k)(Ordering.by[(String,Int),Int](_._2))
    topK.foreach(println)
    sc.stop()
  }
}
