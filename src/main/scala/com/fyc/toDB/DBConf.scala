package com.fyc.toDB

import com.fyc.config.globalConfUtils

object DBConf {


  def url={

    globalConfUtils.mysql_url

  }
  def prop={

    val prop = new java.util.Properties
    prop.setProperty("user", globalConfUtils.mysql_username)
    prop.setProperty("password", globalConfUtils.mysql_password)
    prop

  }

}
