/**
 * FileName: JDBCUtils
 * Author:   DFJX
 * Date:     2019/10/9 10:02
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.fyc.utils;


import com.fyc.config.globalConfUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author DFJX
 * @create 2019/10/9
 * @since 1.0.0
 */
public class JDBCUtils {

    private static Connection getConnection(){

        globalConfUtils globalConfUtils = new globalConfUtils();
        String UTL = globalConfUtils.mysql_url();
        String name = globalConfUtils.mysql_username();
        String Password = globalConfUtils.mysql_password();
        try{
            Class.forName(globalConfUtils.mysql_driver());//加载驱动
            Connection coon = DriverManager.getConnection(UTL,name,Password);//创建连接对象
            return coon;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    
    public static void dropAll(String tbName)throws Exception{
        String sql = "delete from  "+tbName+" where 1=1";
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.execute(sql);
        conn.close();
    }
    public static void dropByday(String tbName,String date)throws Exception{
        String sql = "delete from  "+tbName+" where data_time = '"+date+"'";
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.execute(sql);
        conn.close();
    }



}
