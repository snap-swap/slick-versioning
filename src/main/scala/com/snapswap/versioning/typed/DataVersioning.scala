package com.snapswap.versioning.typed

import java.sql.Timestamp
import java.time.LocalDateTime

import com.snapswap.versioning.abstractt.Versioning


object DataVersioning extends TypedVersioning {

  //must returns current timestamp
  override protected def now(): VersionDt =
    VersionDt.nowUTC()

  //must returns random unique VersionId
  override protected def randomVersion(): VersionId =
    VersionId.random()
}

trait TypedVersioning
  extends Versioning[String, String, LocalDateTime, Timestamp]
    with TypedTypes
