/**
 * FileName: heatTrend
 * Author:   DFJX
 * Date:     2019/9/11 14:25
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.fyc.pojo;



/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author DFJX
 * @create 2019/9/11
 * @since 1.0.0
 */
public class heatTrend {

    //商品id
    private int commid;
    //商品名称
    private String commName;
    //日期
    private String date;
    private String year;
    private String month;
    private String day;

    //正
    private int pos_trend;
    //负
    private int neg_trend;
    //中立
    private int neu_trend;
    //总
    private int heat_trend;

    public heatTrend(String name_id, String key, String value) {
        String[] strings = name_id.split("_");
        this.commid = Integer.parseInt(strings[1]);
        this.commName= strings[0];
        this.date = key;
        String[] split = key.split("-");
        this.year = split[0];
        this.month = split[1];
        this.day = split[2];
        String[] strs = value.split(",");
        this.pos_trend = Integer.parseInt(strs[0]);
        this.neg_trend = Integer.parseInt(strs[1]);
        this.neu_trend = Integer.parseInt(strs[2]);
        setHeat_trend(this.pos_trend+this.neg_trend+this.neu_trend);
    }

    public int getCommid() {
        return commid;
    }

    public void setCommid(int commid) {
        this.commid = commid;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
        String[] split = this.date.split("-");
        this.year = split[0];
        this.month = split[1];
        this.day = split[2];
    }

    public int getPos_trend() {
        return pos_trend;
    }

    public void setPos_trend(int pos_trend) {
        this.pos_trend = pos_trend;
    }

    public int getNeg_trend() {
        return neg_trend;
    }

    public void setNeg_trend(int neg_trend) {
        this.neg_trend = neg_trend;
    }

    public int getNeu_trend() {
        return neu_trend;
    }

    public void setNeu_trend(int neu_trend) {
        this.neu_trend = neu_trend;
    }

    public int getHeat_trend() {
        return heat_trend;
    }

    public void setHeat_trend(int heat_trend) {
        this.heat_trend = heat_trend;
    }

    @Override
    public String toString() {
        return  commid + "," +
                commName + "," +
                pos_trend + "," +
                neg_trend + "," +
                neu_trend + "," +
                heat_trend+ "," +
                date + "," +
                year + "," +
                month + "," +
                day ;
    }
}
