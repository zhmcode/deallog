package com.sparksql.log.utils

import com.ggstar.util.ip.IpHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * Created by zhmcode on 2018/12/4 0004.
  */
object AccessUtils {

    val struct = StructType{
        List(
            StructField("ip",StringType),
            StructField("city",StringType),
            StructField("time",StringType),
            StructField("day",StringType),
            StructField("url",StringType),
            StructField("cmsType",StringType),
            StructField("cmsId",LongType),
            StructField("traffic",LongType)
        )
    }

    def parseWords(line:String):Row = {
        try{
            //ip + "\t" + time  + "\t" + traffic  + "\t" + url
            val splits = line.split("\t")
            val ip = splits(0)
            val city = IpHelper.findRegionByIp(ip)
            val time = splits(1)
            var times = time.split(" ")
            var day = times(0).replaceAll("-", "")
            val traffic = splits(2).toLong
            val url = splits(3)
            var hostname = "http://www.imooc.com/"
            var cmsId = 0l
            var cmsType = ""
            if (url.length > 20) {
                val lastStr = url.substring(hostname.length)
                val words = lastStr.split("/")
                cmsType = words(0)
                cmsId = words(1).toLong
            }
            Row(ip,city,time,day,url,cmsType,cmsId,traffic)
        }catch{
            case e:Exception=>{
                e.printStackTrace()
                Row("","","","","","",0l,0l)
            }
        }
    }

    def main(args: Array[String]): Unit = {
        val city = IpHelper.findRegionByIp("183.162.52.7")
        println(city)
    }
}
