package com.sparksql.log.dao

import java.sql.{Connection, PreparedStatement}
import com.sparksql.log.bean.{CityVideoVisitBean, DayVideoVisitBean, TrafficVideoVisitBean}
import com.sparksql.log.utils.SqlUtils
import scala.collection.mutable.ListBuffer

/**
  * Created by zhmcode on 2018/12/4 0004.
  */
object AccessTopnDao {

    /**
      * 批量插入最受欢迎的TopN课程
      *
      * @param list
      */
    def insertDayVideoAccessTopN(list: ListBuffer[DayVideoVisitBean]) {
        var conneciton: Connection = null
        var pstmt: PreparedStatement = null
        try {
            conneciton = SqlUtils.getConnection()
            //设置手动提交
            conneciton.setAutoCommit(false)
            val sql = "insert into day_video_access_topn (day,cmsId,times) values(?,?,?)"
            pstmt = conneciton.prepareStatement(sql)
            for (ele <- list) {
                pstmt.setString(1, ele.day)
                pstmt.setLong(2, ele.cmsId)
                pstmt.setLong(3, ele.times)
                pstmt.addBatch()
            }
            // 执行批量处理
            pstmt.executeBatch()
            conneciton.commit() //手工提交
        } catch {
            case e: Exception => {
                e.printStackTrace()
            }
        } finally {
            SqlUtils.releaseSource(conneciton, pstmt)
        }
    }

    /**
      * 批量插入按照地市进行统计TopN课程
      *
      * @param list
      */
    def insertCityVideoAccessTopN(list: ListBuffer[CityVideoVisitBean]) {
        var connection: Connection = null
        var pstmt: PreparedStatement = null
        try {
            connection = SqlUtils.getConnection()
            val sql = "insert into city_video_access_topn (city,cmsId,times) values(?,?,?)"
            connection.setAutoCommit(false)
            pstmt = connection.prepareStatement(sql)
            for (ele <- list) {
                pstmt.setString(1, ele.city)
                pstmt.setLong(2, ele.cmsId)
                pstmt.setLong(3, ele.times)
                pstmt.addBatch()
            }
            pstmt.executeBatch()
            connection.commit()
        } catch {
            case e: Exception => {
                e.printStackTrace()
            }
        } finally {
            SqlUtils.releaseSource(connection, pstmt)
        }
    }

    /**
      * 批量插入按照流量进行统计
      *
      * @param list
      */
    def insertTrafficVideoAccessTopN(list: ListBuffer[TrafficVideoVisitBean]) {
        var connection: Connection = null;
        var pstm: PreparedStatement = null;
        try{
            connection = SqlUtils.getConnection()
            val sql = "insert into traffic_video_access_topn (day,cmsId,traffic) values(?,?,?)"
            pstm = connection.prepareStatement(sql)
            connection.setAutoCommit(false)
            for(ele <- list){
                pstm.setString(1,ele.day)
                pstm.setLong(2,ele.cmsId)
                pstm.setLong(3,ele.traffic)
                pstm.addBatch()
            }
            pstm.executeBatch()
            connection.commit()
        }catch {
            case e:Exception =>{
                e.printStackTrace()
            }
        }finally {
            SqlUtils.releaseSource(connection,pstm)
        }
    }

}
