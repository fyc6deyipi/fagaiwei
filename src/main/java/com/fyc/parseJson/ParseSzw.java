/**
 * FileName: ParseSzw
 * Author:   DFJX
 * Date:     2019/12/19 11:27
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.fyc.parseJson;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author DFJX
 * @create 2019/12/19
 * @since 1.0.0
 */
public class ParseSzw {

    public static List<String> runByDay(String jsonStr){
        System.out.println(jsonStr);
        List list  = new ArrayList<String>();
        JSONObject json = JSON.parseObject(jsonStr);
        String date = json.getString("datano");
        String[] split = date.split("-");
        String year = split[0];
        String month = split[1];
        String day = split[2];
        String end = ","+year+","+month+","+day;
        String nation = json.getString("nation");
        if(json.getString("result").equals("1")){
            JSONArray jsonArray = json.getJSONArray("provice");
            for (int i = 0; i < jsonArray.size(); i++) {
                String str = "";
                JSONArray array = (JSONArray) jsonArray.get(i);
                for (Object s : array) {
                    str+=s+",";
                }
                list.add(str+date+end);
            }

        }
        list.add("nation,"+nation+","+date+end);

        return list;

    }


    public static List<String> runByWeek(String jsonStr,String date){
        List list  = new ArrayList<String>();
        JSONObject json = JSON.parseObject(jsonStr);
        String[] split = date.split("-");
        String year = split[0];
        String month = split[1];
        String day = split[2];
        String end = ","+year+","+month+","+day;
        if(json.getString("result").equals("1")) {
            JSONArray jsonArray = json.getJSONArray("weekdata");
            for (int i = 0; i < jsonArray.size(); i++) {
                String str = "";
                JSONArray array = (JSONArray) jsonArray.get(i);
                for (Object s : array) {
                    str+=s+",";
                }
                list.add(str+date+end);
            }
        }




        return list;
    }

}
