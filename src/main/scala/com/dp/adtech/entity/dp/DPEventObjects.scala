package com.dp.adtech.entity.dp

import java.io.{ ByteArrayInputStream, InputStreamReader }
import com.github.tototoshi.csv.CSVReader
import scala.collection.mutable.HashMap

/**
 * Created by miraj on 7/29/16.
 */
trait BaseLogObject {
  def parse(): BaseLogObject

  def parseToCC(): BaseDfObject
}

trait BaseDfObject

/**
 * Wynk payments log class.
 *
 * @param line
 */
class DPPaymentLog(val line: String) extends BaseLogObject {

  var server_timestr: Long = 0
  var ip: String = null
  var user_id: String = null
  var msisdn: String = null
  var xrat: Int = 0
  var circle_id: String = null
  var deviceKey: String = null
  var netop: String = null
  var os: String = null
  var osversion: String = null
  var app_version: String = null
  var activity_type: String = null
  var item_id: String = null
  var action: String = null
  var txnId: String = null
  var price: Double = 0.0
  var status: String = null
  var app: String = null
  var errMsg: String = null
  var paytm: String = null
  var thirdPartyTxnId: String = null

  def parse(): DPPaymentLog = {
    try {
      val stream = new ByteArrayInputStream(line.getBytes)
      val reader = new InputStreamReader(stream, "UTF-8")
      val reader1 = CSVReader.open(reader)
      reader1.foreach {
        fields =>
          try {
            server_timestr = fields.apply(0).toLong
          } catch {
            case e: Exception => server_timestr = 0
          }
          ip = fields.apply(1)
          user_id = fields.apply(2)
          msisdn = fields.apply(3)
          try {
            xrat = fields.apply(4).toInt
          } catch {
            case e: Exception => xrat = 0
          }
          circle_id = fields.apply(5)
          deviceKey = fields.apply(6)
          netop = fields.apply(7)
          os = fields.apply(8)
          osversion = fields.apply(9)
          app_version = fields.apply(10)
          activity_type = fields.apply(11)
          item_id = fields.apply(12)
          action = fields.apply(13)
          txnId = fields.apply(14)
          try {
            price = fields.apply(15).toDouble
          } catch {
            case e: Exception => price = 0.0
          }
          status = fields.apply(16)
          app = fields.apply(17)
          errMsg = fields.apply(18)
          paytm = fields.apply(19)
          thirdPartyTxnId = fields.apply(20)
      }
    } catch {
      case e: Exception => server_timestr = 0
    }
    this
  }

  def parseToCC(): DPPaymentDF = {
    var paymentObj: DPPaymentLog = this
    DPPaymentDF(paymentObj.server_timestr, paymentObj.user_id, paymentObj.msisdn, paymentObj.xrat, paymentObj.circle_id, paymentObj.netop, paymentObj.os, paymentObj.osversion, paymentObj.app_version, paymentObj.activity_type, paymentObj.item_id, paymentObj.action, paymentObj.txnId, paymentObj.price, paymentObj.status, paymentObj.app, paymentObj.errMsg, paymentObj.paytm, paymentObj.thirdPartyTxnId)
  }
}

case class DPPaymentDF(timestamp: Long,
  user_id: String,
  msisdn: String,
  xrat: Int,
  circle_id: String,
  netop: String,
  os: String,
  osversion: String,
  app_version: String,
  activity_type: String,
  item_id: String,
  action: String,
  txnId: String,
  price: Double,
  status: String,
  app: String,
  errMsg: String,
  paytm: String,
  thirdPartyTxnId: String) extends BaseDfObject

/**
 * Wynk User events log class.
 *
 * @param line
 */
class DPUserLog(val line: String) extends BaseLogObject {

  var timestr: Long = 0
  var ip: String = null
  var user_id: String = null
  var xrat: Int = 0
  var circle_id: String = null
  var language_id: String = null
  var device_key: String = null
  var netop: String = null
  var os: String = null
  var osversion: String = null
  var app_version: String = null
  var screen_id: String = null
  var search_keyword: String = null
  var item_id: String = null
  var activity_type: String = null
  var action_id: String = null
  var action_param: String = null
  var app: String = null
  var server_timestr: Long = 0
  var content_lang: String = null
  var device_id: String = null
  var user_type: String = null

  def parse(): DPUserLog = {
    val stream = new ByteArrayInputStream(line.getBytes)
    val reader = new InputStreamReader(stream, "UTF-8")
    val reader1 = CSVReader.open(reader)

    reader1.foreach {
      fields =>
        try {
          timestr = fields.apply(0).toLong
        } catch {
          case e: Exception => timestr = 0
        }
        ip = fields.apply(1)
        user_id = fields.apply(2)
        try {
          xrat = fields.apply(3).toInt
        } catch {
          case e: Exception => xrat = 0
        }
        circle_id = fields.apply(4)
        language_id = fields(5)
        device_key = fields(6)
        netop = fields(7)
        os = fields.apply(8)
        osversion = fields.apply(9)
        app_version = fields(10)
        screen_id = fields(11)
        search_keyword = fields(12)
        item_id = fields(13)
        activity_type = fields(14)
        action_id = fields(15)
        action_param = fields(16)
        app = fields(17)
        try {
          server_timestr = fields.apply(18).toLong
        } catch {
          case e: Exception => server_timestr = 0
        }
        content_lang = fields(19)
        device_id = fields(20)
        user_type = fields(21)
    }
    this
  }

  def parseToCC(): DPUserEvent = {
    var userEventObj: DPUserLog = this
    DPUserEvent(userEventObj.timestr, userEventObj.ip, userEventObj.user_id, userEventObj.xrat, userEventObj.circle_id, userEventObj.language_id, userEventObj.device_key, userEventObj.netop, userEventObj.os, userEventObj.osversion, userEventObj.app_version, userEventObj.screen_id, userEventObj.search_keyword, userEventObj.item_id, userEventObj.activity_type, userEventObj.action_id, userEventObj.action_param, userEventObj.app, userEventObj.server_timestr, userEventObj.content_lang, userEventObj.device_id, userEventObj.user_type)
  }
}

case class DPUserEvent(timestamp: Long, ip: String, user_id: String, xrat: Int, circle_id: String, language_id: String, device_key: String, netop: String, os: String, osversion: String, app_version: String, screen_id: String, search_keyword: String, item_id: String, activity_type: String, action_id: String, action_param: String, app: String, serverTimestamp: Long, content_lang: String, device_id: String, user_type: String) extends BaseDfObject

/**
 * Wynk device log class.
 *
 * @param line
 */
class DPDeviceLog(val line: String) {

  var deviceid: String = null
  var uid: String = null
  var deviceisactive: Boolean = false
  var devicekeylastupdatetime: Long = 0L
  var devicekey: String = null
  var devicetype: String = null
  var deviceappver: String = null
  var lastactivitydate: String = null
  var lastbatchprocessedid: String = null
  var msisdn: String = null
  var operator: String = null
  var osversion: String = null
  var os: String = null
  var deviceimei: String = null
  var deviceregistrationdate: Long = 0L
  var deviceresolution: String = null
  var devicebuildnos: String = null

  def parseToCC(): DPDeviceDetails = {

    println("Passed params: " + line)
    var values = line.split('|')
    var changes: HashMap[String, String] = new HashMap[String, String]()
    values.foreach { x =>
      println("Key value pairs ---" + x)
      val keyValues = x.split('=')
      if (keyValues.length == 2)
        changes.put(keyValues(0).toLowerCase(), keyValues(1))
    }

    println("Final list of attributes to be changed---- : " + changes)
    
    new DPDeviceDetails(changes.getOrElse("deviceid",null),
      changes.getOrElse("uid",null),
      changes.getOrElse("deviceisactive",false).toString().toBoolean,
      changes.getOrElse("devicekeylastupdatetime",0L).toString().toLong,
      changes.getOrElse("devicekey",null),
      changes.getOrElse("devicetype",null),
      changes.getOrElse("deviceappver",null),
      changes.getOrElse("lastactivitydate",null),
      changes.getOrElse("lastbatchprocessedid",null),
      changes.getOrElse("msisdn",null),
      changes.getOrElse("operator",null),
      changes.getOrElse("osversion",null),
      changes.getOrElse("os",null),
      changes.getOrElse("deviceimei",null),
      changes.getOrElse("deviceregistrationdate",0L).toString().toLong,
      changes.getOrElse("deviceresolution",null),
      changes.getOrElse("devicebuildnos",null))
  }

}

case class DPDeviceDetails(
  deviceid: String,
  uid: String,
  deviceisactive: Boolean,
  devicekeylastupdatetime: Long,
  devicekey: String,
  devicetype: String,
  deviceappver: String,
  lastactivitydate: String,
  lastbatchprocessedid: String,
  msisdn: String,
  operator: String,
  osversion: String,
  os: String,
  deviceimei: String,
  deviceregistrationdate: Long,
  deviceresolution: String,
  devicebuildnos: String)
