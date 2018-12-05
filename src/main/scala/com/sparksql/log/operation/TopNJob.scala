package com.sparksql.log.operation

import com.sparksql.log.bean.{CityVideoVisitBean, DayVideoVisitBean, TrafficVideoVisitBean}
import com.sparksql.log.dao.AccessTopnDao
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * Created by zhmcode on 2018/12/4 0004.
  */
object TopNJob {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("TopNJob").master("local[2]").getOrCreate()
        val df = spark.read.format("parquet").load("file:///D:\\test_data\\log\\cleanlog\\*.parquet")
        df.createOrReplaceTempView("tb_access")

        //最受欢迎的TopN课程
         mostPopular(spark, "20161110")
       //按照地市进行统计TopN课程
         mostPopularByCity(spark,"20161110")
         //按照流量进行统计
         mostPopularByTraffic(spark,"20161110")
        spark.stop()
    }

    //Row(ip, url, cmsType, cmsId, traffic, city, time, day)
    def mostPopular(spark: SparkSession,day: String): Unit = {
        val result = spark.sql("select day ,cmsId, count(1) as times from tb_access " +
          "where day = " + day + " and cmsType='video' " +
          "group by cmsId,day order by times desc limit 10 ")
        result.foreachPartition(x => {
            var list = new ListBuffer[DayVideoVisitBean]
            x.foreach(y => {
                val day = y.getAs[String]("day")
                val cmsId = y.getAs[Long]("cmsId")
                val times = y.getAs[Long]("times")
                list.append(DayVideoVisitBean(day, cmsId, times))
            })
            AccessTopnDao.insertDayVideoAccessTopN(list)
        })
    }

    /**
      * 按照地市进行统计TopN课程
      */
    def mostPopularByCity(spark: SparkSession,  day: String): Unit = {
        val result = spark.sql("select city,cmsId,count(1) as times from tb_access where cmsType='video' and day =" + day +
          " group by cmsId,city order by times desc limit 10")
        result.foreachPartition(x => {
            val list = new ListBuffer[CityVideoVisitBean]
            x.foreach(y => {
                val city = y.getAs[String]("city")
                val cmsId = y.getAs[Long]("cmsId")
                val times = y.getAs[Long]("times")
                list.append(CityVideoVisitBean(city,cmsId,times))
            })
            AccessTopnDao.insertCityVideoAccessTopN(list)
        })
    }

    /**
      * 按照流量进行统计
      */
    def mostPopularByTraffic(spark: SparkSession, day: String): Unit = {
        /*  val result = spark.sql("select cmsId,traffic from tv_course where day = " + day + " and cmsType='video' " +
            " group by cmsId,day order by traffic desc ")*/
        val result = spark.sql("select cmsId,day,sum(traffic) as traffics from tb_access where day ='20161110' " +
          "and cmsType='video' group by cmsId,day order by traffics desc limit 10 ")
        result.foreachPartition(x=>{
            val list = new ListBuffer[TrafficVideoVisitBean]
            x.foreach(y=>{
                val cmsId = y.getAs[Long]("cmsId")
                val traffics = y.getAs[Long]("traffics")
                val day = y.getAs[String]("day")
                list.append(TrafficVideoVisitBean(traffics,cmsId,day))
            })
            AccessTopnDao.insertTrafficVideoAccessTopN(list)
        })
    }

}
