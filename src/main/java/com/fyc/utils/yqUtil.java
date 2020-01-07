/**
 * FileName: yqUtil
 * Author:   DFJX
 * Date:     2019/12/19 14:15
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.fyc.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author DFJX
 * @create 2019/12/19
 * @since 1.0.0
 */
public class yqUtil {
    public static Map getMap(){
        HashMap<Integer, String> map = new HashMap<Integer, String>();
        map.put(409,"原油_29");
        map.put(410,"液化天然气_30");
        map.put(411,"天然气_31");
        map.put(412,"煤炭_32");
        map.put(413,"动力煤_33");
        map.put(414,"焦炭_34");
        map.put(415,"焦煤_35");
        map.put(416,"原煤_36");
        map.put(417,"煤及褐煤_37");
        map.put(418,"无烟煤_38");
        map.put(419,"炼焦烟煤_39");
        map.put(420,"褐煤_40");
        map.put(421,"其他烟煤_41");
        map.put(422,"螺纹钢_42");
        map.put(423,"钢材_43");
        map.put(424,"热轧板卷_44");
        map.put(425,"冷轧板卷_45");
        map.put(426,"中厚板_46");
        map.put(427,"线材_47");
        map.put(428,"铁矿石_48");
        map.put(429,"电解铜_49");
        map.put(430,"电解铝_50");
        map.put(431,"水泥_51");
        map.put(432,"玻璃_52");
        map.put(433,"甲醇_53");
        map.put(434,"PTA_54");
        map.put(435,"尿素_55");
        map.put(436,"三元复合肥_56");
        map.put(437,"氯化钾_57");
        map.put(438,"生猪_58");
        map.put(439,"牛肉_59");
        map.put(440,"鸡蛋_60");
        map.put(441,"稻谷_61");
        map.put(442,"大米_62");
        map.put(443,"小麦_63");
        map.put(444,"玉米_64");
        map.put(445,"大豆_65");
        map.put(446,"豆粕_66");
        map.put(447,"棉花_67");
        map.put(448,"大豆油_68");
        map.put(449,"花生油_69");
        map.put(450,"苹果_70");
        map.put(451,"大蒜_71");
        map.put(450,"苹果_72");
        map.put(452,"香蕉_73");
        map.put(453,"橙子_74");
        map.put(454,"梨_75");
        map.put(455,"西瓜_76");
        map.put(456,"土豆_77");
        map.put(457,"西红柿_78");
        map.put(458,"大白菜_79");
        map.put(459,"豆角_80");
        map.put(460,"青椒_81");
        map.put(461,"牛肉_82");
        map.put(462,"羊肉_83");
        map.put(463,"鸡肉_84");
        map.put(464,"猪肉_85");
        map.put(465,"鸡蛋_86");
        map.put(466,"大米_87");
        map.put(467,"面粉_88");
        map.put(468,"大豆_89");
        map.put(469,"大豆油_90");
        map.put(470,"花生油_91");
        map.put(471,"酱油_92");
        map.put(472,"醋_93");
        map.put(473,"食用盐_94");
        map.put(474,"方便面_95");
        map.put(475,"榨菜_96");
        map.put(476,"牛奶_97");
        map.put(477,"矿泉水_98");
        map.put(478,"橙味饮料_99");
        map.put(479,"啤酒_100");
        map.put(480,"饼干_101");
        map.put(481,"巧克力_102");
        map.put(482,"糖果_103");
        map.put(483,"速冻水饺_104");
        map.put(484,"速冻汤圆_105");
        map.put(485,"即食点心_106");
        map.put(486,"洗发水_107");
        map.put(487,"洗洁精_108");
        map.put(488,"洗衣液_109");
        map.put(489,"香皂_110");
        map.put(490,"牙膏_111");
        map.put(491,"卫生纸_112");
        map.put(492,"木耳_113");
        map.put(493,"石油_295");

        return map;
    }

    //合并key相同的map，v拼接
    public static Map<String,String> mapMerge(Map<String,Integer> map1,Map<String,Integer> map2,Map<String,Integer> map3){
        Map<String, String> map = new HashMap<String, String>();
        for (Map.Entry<String, Integer> entry : map1.entrySet()) {
            map.put(entry.getKey(),entry.getValue().toString());
        }
        for (Map.Entry<String, Integer> entry : map2.entrySet()) {
            map.put(entry.getKey(),map.get(entry.getKey())+","+entry.getValue());
        }
        for (Map.Entry<String, Integer> entry : map3.entrySet()) {
            map.put(entry.getKey(),map.get(entry.getKey())+","+entry.getValue());
        }

        return map;
    }

}
