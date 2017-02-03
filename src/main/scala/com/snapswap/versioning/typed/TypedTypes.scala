package com.snapswap.versioning.typed

import java.sql.Timestamp
import java.time.LocalDateTime

import com.snapswap.versioning.abstractt.Types
import com.fasterxml.uuid.Generators
import com.snapswap.versioning.utils.{LocalDateTimeHelper, StringToPostgresHash}
import slick.jdbc.PostgresProfile.api._


trait TypedTypes extends Types[String, String, LocalDateTime, Timestamp] {

  override implicit def dataIdMapper: BaseColumnType[DataId] =
    MappedColumnType.base[DataId, BaseDataId](
      x => x.baseValue,
      x => DataIdClass(x)
    )

  override implicit def versionIdMapper: BaseColumnType[VersionId] =
    MappedColumnType.base[VersionId, BaseDataId](
      x => x.baseValue,
      x => VersionIdClass(x)
    )

  override implicit def versionDtMapper: BaseColumnType[VersionDt] =
    MappedColumnType.base[VersionDt, BaseVersionDt](
      x => Timestamp.valueOf(x),
      x => VersionDt(x)
    )


  case class DataIdClass(value: String) extends DataId {

    override def baseValue: String = value

    override def asDataId: DataId = this
  }

  object DataId {
    /*
    * Here we can use a compound primary key for DataId creation.
    * For avoiding a big string we use a hash function - md5 is a good choice,
    * because we can use the same function in a postgres too (md5($our_string)::varchar).
    * It is VERY convenient if we should do some updates (in migration scripts for example).
    * */
    def surrogate(values: String*): DataId =
      DataIdClass(StringToPostgresHash.md5(values.toSeq.mkString("|")))

    def natural(value: String): DataId =
      DataIdClass(value)
  }


  case class VersionIdClass(value: String) extends VersionId {
    override def baseValue: String = value

    override def asVersionId: VersionId = this
  }

  object VersionId {
    def random(): VersionId =
      VersionIdClass(Generators.timeBasedGenerator.generate().toString)
  }


  object VersionDt {
    def nowUTC(): VersionDt =
      LocalDateTimeHelper.nowUTC()

    def apply(ts: Timestamp): VersionDt =
      ts.toLocalDateTime
  }

}
