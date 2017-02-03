package com.snapswap.versioning

import java.security.MessageDigest
import java.time.{Clock, LocalDateTime}

package object utils {

  object StringToPostgresHash {
    def md5(value: String): String = {
      MessageDigest.getInstance("MD5")
        .digest(value.getBytes())
        .map("%02x".format(_)) //postgres returns result in hexadecimal, we too
        .mkString
    }
  }

  object LocalDateTimeHelper {

    implicit def dateTimeOrdering: scala.Ordering[LocalDateTime] = scala.Ordering.fromLessThan(_ isBefore _)

    def nowUTC() =
      LocalDateTime.now(Clock.systemUTC())
  }

}
