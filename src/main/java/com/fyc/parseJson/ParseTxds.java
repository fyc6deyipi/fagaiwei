/**
 * FileName: ParseTxds
 * Author:   DFJX
 * Date:     2019/12/19 11:31
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.fyc.parseJson;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fyc.config.globalConfUtils;
import com.fyc.utils.dateUtils;

import java.io.FileWriter;
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
public class ParseTxds {
    public static List<String> run(String jsonStr, int frequence) throws Exception {

        List<String> list =  new ArrayList<String>();
        List<String> listDay =  new ArrayList<String>();
        List<String> listWeek =  new ArrayList<String>();
        JSONObject json = JSON.parseObject(jsonStr);
        String end="";
        String today = dateUtils.getToday();



        if ("0".equals(json.getString("code"))) {
            JSONArray array = json.getJSONArray("prices");

            FileWriter out = new FileWriter("C:\\Users\\DFJX\\Desktop\\ds1230.csv", false);

            for (int i = 0; i < array.size(); i++) {
                JSONObject pojo = (JSONObject) array.get(i);
                String goodsId = pojo.getString("goodsId");
                String goodsName = pojo.getString("goodsName");
                String goodsCate = pojo.getString("goodsCate");
                String goodsUnit = pojo.getString("goodsUnit");
                out.write(goodsId+","+goodsName+","+goodsCate+","+goodsUnit+"\n");
            }
            out.close();
        }
        return list;
    }
    public static List<String> run(String jsonStr) throws Exception {

        List<String> list =  new ArrayList<String>();
        JSONObject json = JSON.parseObject(jsonStr);
        String end="";
        String today = dateUtils.getToday();

        String[] arr = today.split("-");
        end=","+today+","+arr[0]+","+arr[1]+","+arr[2];


        if ("0".equals(json.getString("code"))) {
            JSONArray array = json.getJSONArray("prices");

            for (int i = 0; i < array.size(); i++) {
                JSONObject pojo = (JSONObject) array.get(i);
                String goodsId = pojo.getString("goodsId");
                String dayAvgPrice = pojo.getString("dayAvgPrice");
                String dayChainChgRange = pojo.getString("dayChainChgRange");
                String dayChainChgValue = pojo.getString("dayChainChgValue");
                String weekMaxPrice = pojo.getString("weekMaxPrice");
                String weekMinPrice = pojo.getString("weekMinPrice");

                list.add(goodsId+"_dayAvgPrice,"+dayAvgPrice+end);
                list.add(goodsId+"_dayChainChgRange,"+dayChainChgRange.split("%")[0]+end);
                list.add(goodsId+"_dayChainChgValue,"+dayChainChgValue+end);
                if(dateUtils.isWeekN(today,7)){
                    String[] Friday = dateUtils.date2WeekN(today, 5).split("-");
                    end = ","+Friday+","+Friday[0]+","+Friday[1]+","+Friday[2];
                    list.add(goodsId+"_weekMaxPrice,"+weekMaxPrice+end);
                    list.add(goodsId+"_weekMinPrice,"+weekMinPrice+end);
                }
            }
        }
        return list;
    }


}
