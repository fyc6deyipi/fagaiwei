package com.fyc.getJson

import com.alibaba.fastjson.JSONObject
import com.fyc.`trait`.GetJson
import com.fyc.config.globalConfUtils
import com.fyc.utils.dateUtils
import org.apache.http.{Header, HttpEntity}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object GtwJson extends GetJson{

  def get(arr: Array[String]):String={
    val jsonObj = new JSONObject
    jsonObj.put("token", globalConfUtils.token)
    jsonObj.put("indexcodes", arr)
    jsonObj.put("starttime", globalConfUtils.startTime)
    jsonObj.put("endtime", dateUtils.getYesterTimestamp.toLong+24*60*60*1000-1)

    jsonObj.put("order", globalConfUtils.order)

    val url = globalConfUtils.gtwUrl
    val client = HttpClients.createDefault()
    val post: HttpPost = new HttpPost(url)
    //设置提交参数为application/json
    post.addHeader("Content-Type", "application/json")
    post.setEntity(new StringEntity(jsonObj.toJSONString))
    println(jsonObj.toJSONString)
    //执行请求
    val response: CloseableHttpResponse = client.execute(post)
    //返回结果
    val allHeaders: Array[Header] = post.getAllHeaders
    val entity: HttpEntity = response.getEntity
    val string = EntityUtils.toString(entity, "UTF-8")

    string


  }
  def get(arr: Array[String],startTime:String,endTime:String):String={
    val jsonObj = new JSONObject
    jsonObj.put("token", globalConfUtils.token)
    jsonObj.put("indexcodes", arr)
    jsonObj.put("starttime", startTime)
    if ("".equals(endTime)){
      jsonObj.put("endtime", startTime.toLong+24*60*60*1000-1)
    }else {

      jsonObj.put("endtime", endTime.toLong+24*60*60*1000-1)
    }


    jsonObj.put("order", globalConfUtils.order)

    val url = globalConfUtils.gtwUrl
    val client = HttpClients.createDefault()
    val post: HttpPost = new HttpPost(url)
    //设置提交参数为application/json
    post.addHeader("Content-Type", "application/json")
    post.setEntity(new StringEntity(jsonObj.toJSONString))
    println(jsonObj.toJSONString)
    //执行请求
    val response: CloseableHttpResponse = client.execute(post)
    //返回结果
    val allHeaders: Array[Header] = post.getAllHeaders
    val entity: HttpEntity = response.getEntity
    val string = EntityUtils.toString(entity, "UTF-8")
    println(string)
    string


  }


}
