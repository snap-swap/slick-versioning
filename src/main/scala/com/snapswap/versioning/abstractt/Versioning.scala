package com.snapswap.versioning.abstractt

import slick.jdbc.PostgresProfile.api._
import slick.lifted.{CanBeQueryCondition, Tag}
import slick.sql.SqlProfile.ColumnOption.{NotNull, Nullable}
import scala.concurrent.ExecutionContext


trait Versioning[DataIdDatabaseType, VersionIdDatabaseType, VersionDtType, VersionDtDatabaseType] {
  self: Types[DataIdDatabaseType, VersionIdDatabaseType, VersionDtType, VersionDtDatabaseType] =>

  //must returns current timestamp
  protected def now(): VersionDt

  //must returns random unique VersionId
  protected def randomVersion(): VersionId


  /*
  * Base trait for versioned data representation
  * */
  trait HistoricalData[D] {
    def toData: D

    def dataId: DataId

    def versionId: VersionId

    def versionCreatedAt: VersionDt

    def versionDeletedAt: Option[VersionDt]

    def equalTo[H <: HistoricalData[D]](other: H): Boolean =
      this.toData.hashCode() == other.toData.hashCode()

    final def notEqualTo[H <: HistoricalData[D]](other: H): Boolean =
      !equalTo(other)

    final def isActual: Boolean =
      versionDeletedAt.isEmpty
  }


  /*
  * Here is a "create table" script example with historical columns only.
  * It's also contains minimum required set of constraints and indexes
  *
   * create table historical_table(
   *  id varchar not null,
   *  version varchar not null,
   *  constraint PK_historical_table primary key (id, version),
   *  created_at timestamp not null,
   *  deleted_at timestamp null
   *  -- other columns
   * );
   *
   * create unique index idx_historical_table_id on historical_table(id) where deleted_at is null; -- prevents more than one actual version per id
   *
  * */
  abstract class HistoricalTable[D, H <: HistoricalData[D]](tag: Tag,
                                                            table: String,
                                                            schema: Option[String] = None,
                                                            idColumnName: String = "id",
                                                            versionIdColumnName: String = "version",
                                                            createdAtColumnName: String = "created_at",
                                                            deletedAtColumnName: String = "deleted_at") extends Table[H](tag, schema, table: String) {

    final def dataId = column[DataId](idColumnName, NotNull)

    final def versionId = column[VersionId](versionIdColumnName, NotNull)

    final def versionCreatedAt = column[VersionDt](createdAtColumnName, NotNull)

    final def versionDeletedAt = column[Option[VersionDt]](deletedAtColumnName, Nullable)

    final def isDeleted: Rep[Boolean] = versionDeletedAt.isDefined

    final def isActual: Rep[Boolean] = versionDeletedAt.isEmpty
  }


  /*
  * These methods allows to work with a historical table like with the usual one
  * */
  trait HistoricalSql[D, H <: HistoricalData[D], T <: HistoricalTable[D, H]] {

    implicit class TableImporter(table: TableQuery[T]) {

      /*
      * SELECT OPERATORS, output result contains only actual versions
      * */
      private def selectActualVersionsQuery(dataset: Query[T, H, Seq]): Query[T, H, Seq] = {
        dataset.filter(_.isActual)
      }

      def hSelect[R <: Rep[_]](where: T => R)(implicit wt: CanBeQueryCondition[R]): Query[T, H, Seq] =
        selectActualVersionsQuery(table.filter(where))

      def hSelectAll[R <: Rep[_]]: Query[T, H, Seq] =
        selectActualVersionsQuery(table)


      /*
      * hDelete delete *actual versions* by given condition
      * */
      private def deleteQuery(dataset: Query[T, H, Seq], deleteDt: VersionDt = now())
                             (implicit ctx: ExecutionContext): DBIO[Seq[(DataId, VersionId)]] = for {
        toDelete <- selectActualVersionsQuery(dataset).forUpdate.map { v => (v.dataId, v.versionId) }.result
        _ <- DBIO.sequence(toDelete.map { case (id, _) =>
          selectActualVersionsQuery(table.filter(_.dataId === id)).map(_.versionDeletedAt).update(Some(deleteDt))
        })
      } yield toDelete

      def hDelete[R <: Rep[_]](where: T => R)
                              (implicit wt: CanBeQueryCondition[R],
                               ctx: ExecutionContext): DBIO[Seq[(DataId, VersionId)]] =
        deleteQuery(table.filter(where))


      /*
      * transformVersion allows transform historical fields for any version
      * */
      private def transformVersion(data: H, dataId: DataId, version: VersionId, createdAt: VersionDt, deletedAt: Option[VersionDt]): H = {

        //TODO consider initing instance from tuple avoiding a hard reflection

        val `class` = data.getClass
        val constructor = `class`.getConstructors.head
        val fields = `class`.getDeclaredFields.take(constructor.getParameterTypes.length)

        val params: Array[AnyRef] =
          fields
            .map { f =>
              f.setAccessible(true)

              (f.getName match {
                case "dataId" => dataId
                case "versionId" => version
                case "versionCreatedAt" => createdAt
                case "versionDeletedAt" => deletedAt
                case _ => f.get(data)
              }).asInstanceOf[AnyRef]
            }

        constructor.newInstance(params: _*).asInstanceOf[H]
      }

      /*
      * asDeletedVersion transforms any *version* to the *deleted version*
      * */
      private def asDeletedVersion(data: H, forceId: DataId, forceVersion: VersionId, deletedAt: VersionDt): H =
        transformVersion(data, forceId, forceVersion, data.versionCreatedAt, Some(deletedAt))

      /*
      * asActualVersion transforms any *version* to the *actual version*
      * */
      private def asActualVersion(data: H, forceId: DataId): H =
        transformVersion(data, forceId, randomVersion(), now(), None)


      /*
       * INSERT OPERATOR, hInsert inserts *actual version*
       * unique index on (dataId) where versionDeletedAt is null should prevent duplicates, otherwise error will be thrown by database,
       * so we don't do any checks here
       * */
      private def insertQuery(data: H)(implicit ctx: ExecutionContext): DBIO[H] = {
        (table += data) map (_ => data)
      }

      def hInsert(data: H)(implicit ctx: ExecutionContext): DBIO[H] = {
        insertQuery(asActualVersion(data, data.dataId))
      }


      /*
      * UPDATE OPERATORS
      *
      * In concurrent updates for the same data we use row-level locks to ensure data consistency (select for update).
      * But we can't remove an *actual version* and add the *new* one,
      * because concurrent update operation won't see any data after row will be released after lock.
      * It happens due concurrent operation performs selection with *where deleted_at is null* condition,
      * but deleted_at was updated by other operation at a some not null value.
      *
      * We'll use a trick: instead of deleting an *actual version* we updating it to a new value, so we keep it *actual*,
      * and instead of inserting a *new version* we inserting a *deleted version*,
      * so concurrent update will be able to see an *updated actual version*.
      * */
      private def updateQuery(dataset: Query[T, H, Seq])(updSet: H => H)(implicit ctx: ExecutionContext): DBIO[Seq[H]] = (for {
        toUpdate <- selectActualVersionsQuery(dataset).forUpdate.result
        versionSet = toUpdate.map { v => (v, updSet(v)) }.collect {
          case (current, updated) if current notEqualTo updated =>
            (current, asActualVersion(updated, current.dataId))
        }
        updateActual <- DBIO.sequence(versionSet.map { case (current, updated) =>
          selectActualVersionsQuery(table.filter(_.dataId === current.dataId)).update(updated)
        })
        addDeleted <- DBIO.sequence(versionSet.map { case (current, updated) =>
          insertQuery(asDeletedVersion(current, current.dataId, current.versionId, updated.versionCreatedAt))
        })
        result = versionSet.map { case (_, updated) => updated }
      } yield result).transactionally

      /*
      * hUpdate like a classic update operation can performs an update on arbitrary rows.
      * Certainly, only actual versions well be participated in search and update operations.
      * But technically we can update a dataId or others historical columns too.
      * To prevent this abnormal situation we use transformVersion method wrappers to performing a force update
      * for a dataId by the original value from the existing version.
       */
      def hUpdate[R <: Rep[_]](where: T => R)(updData: H => H)(implicit wt: CanBeQueryCondition[R],
                                                               ctx: ExecutionContext): DBIO[Seq[H]] =
        updateQuery(table.filter(where))(updData)

      /*
      * hUpdateRow updates only one corresponding row entirely.
      * */
      def hUpdateRow(updData: H)(implicit ctx: ExecutionContext): DBIO[Option[H]] =
        hUpdate(_.dataId === updData.dataId)(_ => updData).map(_.headOption)

      /*
      * combined update-insert operator
      * */
      def hUpdateOrInsert(data: H)(implicit ctx: ExecutionContext): DBIO[Option[H]] = (for {
        exists <- hSelect(_.dataId === data.dataId).forUpdate.exists.result
        result <- if (exists)
          hUpdateRow(data)
        else
          hInsert(data).map(Some(_))
      } yield result).transactionally
    }

  }

}
