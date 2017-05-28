package com.dp.adtech.entity.dp

import org.apache.spark.sql.Row
/**
 * @author miraj
 */

case class UserDetails(uid: String, fname: String, lname: String, email: String, dob: String, gender: String,
  msisdn: String, circle: String, operator: String, usertype: String,
  registration_date: String, lang_selcted: String, songquality: String, user_subscription_type: String,
  lastactivitydate: String, rentalscount: Long, streamedcount: Long,
  daily_avg_song_streamed: Long, song_streamed_30_days: Long, max_mrp: String)

case class UserSongPlayed(uid: String, lastactivitydate: String)

case class PlayCount(uid: String, day: String, stream_count: Long)


case class DeviceDetails(
deviceid: String, uid : String , deviceisactive: Boolean,
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
devicebuildnos: String
);

  




