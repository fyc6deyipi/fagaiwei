package com.fyc.getJson

import com.fyc.`trait`.GetJson
import com.fyc.config.globalConfUtils
import com.fyc.utils.dateUtils
import org.apache.commons.codec.digest.DigestUtils
import org.apache.http.{Header, HttpEntity}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import scala.util.Random

object TxdsJson extends GetJson {

  def get(): String ={
    val ts =  dateUtils.getUnixTimestamp
    val token: String = DigestUtils.md5Hex(globalConfUtils.secretKey+ts)
    val nonce: Int = new Random().nextInt(1000000)+1
    val secretId = 1
    val url = globalConfUtils.txdsUrl+"?ts="+ts+"&token="+token+"&secretId="+secretId+"&nonce="+nonce
    val client = HttpClients.createDefault()
    val post: HttpPost = new HttpPost(url)
    //设置提交参数为application/json
    post.addHeader("Content-Type", "application/json")
    post.setEntity(new StringEntity(globalConfUtils.requestJson))
    println(globalConfUtils.requestJson)
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
