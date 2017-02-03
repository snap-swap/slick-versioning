package com.snapswap.versioning.abstractt

import slick.jdbc.PostgresProfile.api._

trait Types[DataIdDatabaseType, VersionIdDatabaseType, VersionDtType, VersionDtDatabaseType] {

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


  /*
  * Slick base type converters
  * */
  implicit def dataIdMapper: BaseColumnType[DataId]

  implicit def versionIdMapper: BaseColumnType[VersionId]

  implicit def versionDtMapper: BaseColumnType[VersionDt]
}