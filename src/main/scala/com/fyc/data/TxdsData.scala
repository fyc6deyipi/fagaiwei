package com.fyc.data

import com.fyc.`trait`.GetData
import com.fyc.getJson.TxdsJson
import com.fyc.parseJson.ParseTxds

class TxdsData  extends GetData{
  def get()={
    val str: String = TxdsJson.get()
    ParseTxds.run(str)
  }
}
object TxdsData extends TxdsData
