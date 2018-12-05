package com.sparksql.log.operation

import com.sparksql.log.utils.AccessUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by zhmcode on 2018/12/4 0004.
  */
object LogCleanJob {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("LogCleanJob").master("local[2]").getOrCreate()
        val rdd = spark.sparkContext.textFile("file:///D:\\test_data\\log\\formatlog\\part-*")
        val rowRdd = rdd.map(x => {
            AccessUtils.parseWords(x)
        })
        val df = spark.createDataFrame(rowRdd,AccessUtils.struct)
        df.show(false)
     /*   //.partitionBy("day")
        df.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).save("file:///D:\\test_data\\log\\cleanlog")*/
        spark.stop()
    }
}
