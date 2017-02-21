package com.snapswap.versioning.abstractt

import slick.ast.BaseTypedType
import slick.jdbc.{JdbcProfile, JdbcType}

trait Types[DataIdDatabaseType, VersionIdDatabaseType, VersionDtType, VersionDtDatabaseType] extends JdbcProfile {

  object API extends API

  protected type BaseDataId = DataIdDatabaseType
  protected type BaseVersionId = VersionIdDatabaseType
  protected type BaseVersionDt = VersionDtDatabaseType

  type DataId = DataIdType[BaseDataId]
  type VersionId = VersionIdType[BaseVersionId]
  type VersionDt = VersionDtType


  protected trait DataIdType[B] {
    def baseValue: B

    def asDataId: DataId
  }

  protected trait VersionIdType[B] {
    def baseValue: B

    def asVersionId: VersionId
  }


  //type for implicit conversion scala to jdbc types
  type MapperType[T] = JdbcType[T] with BaseTypedType[T]
}