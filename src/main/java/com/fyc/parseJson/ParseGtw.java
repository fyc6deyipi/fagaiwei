/**
 * FileName: ParseGtw
 * Author:   DFJX
 * Date:     2019/12/19 11:23
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
public class ParseGtw {
    public static List<String> run(String jsonStr,int frequence_id) throws Exception{
        List<String> list =  new ArrayList<String>();
        JSONObject json = JSON.parseObject(jsonStr);
        /*
        {"code":"0",
        "msg":"处理成功",
        "total":1,
        "result":[{"indexcode":"CA_0002454337","indexname":"中国煤炭进口数量：加拿大：累计值（月）","unit":"吨","datatype":"月","sourcename":"中国海关总署","description":"","begindate":"2004-01-31","enddate":"2019-06-30","updatestatus":1,"extends":{"rating":"","filed-1":"万吨","filed-2":"月","filed-3":"煤炭进口量-加拿大","filed-4":"","filed-5":"","filed-6":"","filed-7":""},"datalist":[{"value":"2535385.901","date":"2018-09-30","createtime":1542768780424},{"value":"2107005.31","date":"2018-08-31","createtime":1542763845088},{"value":"1596908.564","date":"2018-07-31","createtime":1542701424993},{"value":"1175669.414","date":"2018-06-30","createtime":1542698409869},{"value":"828766.87","date":"2018-05-31","createtime":1542696163393}]}]}
        * */

        if ("0".equals(json.getString("code"))){
            JSONArray result = json.getJSONArray("result");
            for (int i = 0; i < json.getInteger("total"); i++) {
                JSONObject data = (JSONObject) result.get(i);
                JSONArray datalist = data.getJSONArray("datalist");

                String indexcode = data.getString("indexcode");

                for (int j = 0; j < datalist.size(); j++) {
                    globalConfUtils globalConfUtils = new globalConfUtils();
                    String line = "";
                    JSONObject detail_data = (JSONObject) datalist.get(j);
                    String value = detail_data.getString("value");
                    String date = detail_data.getString("date");
                    if (frequence_id == globalConfUtils.week()){
                        date = dateUtils.date2WeekN(date,5);
                    }else if (frequence_id == globalConfUtils.mouth()){
                        date = dateUtils.date2MonthEnd(date);
                    }
                    String[] strings = date.split("-");
                    String year = strings[0];
                    String month = strings[1];
                    String day = strings[2];
                    line = indexcode+","+value+","+date+","+year+","+month+","+day;
                    list.add(line);

                }
            }
        }

        return list;
    }


}
