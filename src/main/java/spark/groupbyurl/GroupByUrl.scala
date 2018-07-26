package spark.groupbyurl

import org.apache.hadoop.hdfs.DFSClient.Conf
import org.apache.spark.{SparkConf, SparkContext}

object GroupByUrl {
  def main(args: Array[String]): Unit = {
    val config=new SparkConf().setAppName("groupbyurl").setMaster("local[2]")
    val sc=new SparkContext(config)
    sc.textFile("hdfs://mini01:9000/spark/itcast").map(x=>(x.split("/t"))).map(arr=>(arr(1),1)).reduceByKey(_+_)
      .sortBy(_._2,false).saveAsHadoopFile("hdfs://mini01:9000/spark/out1")
    sc.stop()
  }
}
