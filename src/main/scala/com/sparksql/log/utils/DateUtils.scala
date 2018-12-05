package com.sparksql.log.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 输入日期格式：10/Nov/2016:00:01:02 +0800 输出日期格式：yyyy-MM-dd HH:mm:s
  */
object DateUtils {

    val INPUT_FORMATE = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

    val OUTPUT_FORMATE  = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

    /**
      * 解析出
      * yyyy-MM-dd HH:mm:ss 时间格式
      */
    def parse(tiemStr : String):String ={
        OUTPUT_FORMATE.format(new Date(getTime(tiemStr)))
    }

    def getTime(tiemStr: String): Long = {
        try{
            INPUT_FORMATE.parse(tiemStr).getTime
        }catch {
            case e:Exception =>{
                e.printStackTrace()
                0l
            }
        }
    }

    def main(args: Array[String]): Unit = {
        val result = parse("10/Nov/2016:00:01:02 +0800")
        println(result)
    }

}
