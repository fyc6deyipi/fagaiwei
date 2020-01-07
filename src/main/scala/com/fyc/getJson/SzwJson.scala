package com.fyc.getJson

import com.fyc.`trait`.GetJson
import com.fyc.config.globalConfUtils
import org.apache.http.{Header, HttpEntity}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

object SzwJson extends GetJson{


  def get(sign:String):String={
    val url = globalConfUtils.szwUrl
    val client = HttpClients.createDefault()
    val post: HttpPost = new HttpPost(url)
    //设置提交参数为application/json
    post.addHeader("Content-Type", "application/x-www-form-urlencoded")
    post.setEntity(new StringEntity(sign))
    //执行请求
    val response: CloseableHttpResponse = client.execute(post)
    //返回结果
    val allHeaders: Array[Header] = post.getAllHeaders
    val entity: HttpEntity = response.getEntity
    val string = EntityUtils.toString(entity, "UTF-8")

    string
  }

}
