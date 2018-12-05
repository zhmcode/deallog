package com.sparksql.log.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Created by zhmcode on 2018/12/4 0004.
  */
object SqlUtils {

    /**
      * 获取连接
      * @return
      */
    def getConnection(): Connection ={
        DriverManager.getConnection("jdbc:mysql://192.168.126.31:3306/sparklog?user=root&password=Zhm@818919")
    }

    /**
      * 释放资源
      * @param conn
      * @param pst
      */
    def releaseSource(conn:Connection,pst:PreparedStatement): Unit ={
        try{
            if(pst!=null){
                pst.close()
            }
        }catch{
            case e:Exception=>{
                e.printStackTrace()
            }
        }finally {
            if(conn!=null){
                conn.close()
            }
        }
    }
}
