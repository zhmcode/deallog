package com.sparksql.log.operation

import com.sparksql.log.utils.DateUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by zhmcode on 2018/12/4 0004.
  */
object LogFormatJob {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("LogFormatJob").master("local[2]").getOrCreate()
        val rdd = spark.sparkContext.textFile("file:///D:\\test_data\\log\\mooc_access.log")
        val rdd1 = rdd.map(x => {
            val splits = x.split(" ")
            if(splits.length>10){
                val ip = splits(0)
                // [10/Nov/2016:00:01:02 +0800]
                var time = splits(3) + " " + splits(4)
                time =  time.substring(time.indexOf("[")+1,time.lastIndexOf("]"))
                time = DateUtils.parse(time)

                var traffic = splits(9)

                var url = splits(11).replaceAll("\"","")

                ip + "\t" + time  + "\t" + traffic  + "\t" + url
            }
        })
        rdd1.saveAsTextFile("file:///D:\\test_data\\log\\formatlog")
        spark.stop()
    }
}
