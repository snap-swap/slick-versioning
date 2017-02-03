package com.snapswap.versioning

import java.time.LocalDateTime
import java.util.UUID

import com.snapswap.versioning.typed.DataVersioning._
import com.snapswap.versioning.utils.LocalDateTimeHelper._
import org.postgresql.util.PSQLException
import org.scalatest.{Matchers, WordSpecLike}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import slick.jdbc.PostgresProfile.api._
import slick.lifted.Tag

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class VersioningSpec
  extends WordSpecLike
    with ScalaFutures
    with Matchers
    with TestDb {

  "For any historical table" when {
    "hInsert" should {
      "return inserted version" in new Setup {
        val toInsert = HistoricalTestData.random()

        def action = db.run(table.hInsert(toInsert))

        whenReady(prepare(action)) { inserted =>
          inserted.toData shouldBe toInsert.toData
          inserted.versionDeletedAt shouldBe empty
          inserted.versionId should not be toInsert.versionId
        }
      }
      "insert actual version of data (even if a given data looks not like an actual version)" in new Setup {
        val toInsert = HistoricalTestData.random(versionDeletedAt = Some(VersionDt.nowUTC()))

        def action = db.run(table.hInsert(toInsert))

        whenReady(prepare(action)) { inserted =>
          inserted.toData shouldBe toInsert.toData
          inserted.versionDeletedAt shouldBe empty
        }
      }
      "fails (db error must be thrown) when inserting data with an existing dataId" in new Setup {
        val data1 = HistoricalTestData.random()
        val data2 = HistoricalTestData.random(dataId = data1.dataId)

        def action = for {
          _ <- db.run(table.hInsert(data1))
          _ <- db.run(table.hInsert(data2))
        } yield ()

        whenReady(prepare(action).failed) { result =>
          result shouldBe a[PSQLException]
        }
      }
      "success when inserting data with the same dataId as an earlier existing dataId" in new Setup {
        val data1 = HistoricalTestData.random()
        val data2 = HistoricalTestData.random(dataId = data1.dataId)

        def action = for {
          _ <- db.run(table.hInsert(data1))
          _ <- db.run(table.hDelete(_.dataId === data1.dataId))
          _ <- db.run(table.hInsert(data2))
          select <- db.run(table.result)
        } yield select

        whenReady(prepare(action)) { result =>
          result.length shouldBe 2
        }
      }
    }


    "hDelete" should {
      "return dataId and versionId for deleted versions" in new Setup {
        val data1 = HistoricalTestData.random()
        val data2 = HistoricalTestData.random()

        def action = for {
          _ <- db.run(table.hInsert(data1))
          inserted <- db.run(table.hInsert(data2))
          deleted <- db.run(table.hDelete(_.dataId === data2.dataId))
        } yield (inserted, deleted)

        whenReady(prepare(action)) { case (inserted, deleted) =>
          Seq((inserted.dataId, inserted.versionId)) shouldBe deleted
        }
      }
      "delete only actual versions corresponding a given condition" in new Setup {
        val data1 = HistoricalTestData.random()
        val data2 = HistoricalTestData.random(dataValue = data1.dataValue)
        val data3 = HistoricalTestData.random(dataValue = data1.dataValue)
        val data4 = HistoricalTestData.random()

        def action = for {
          _ <- db.run(table.hInsert(data1))
          _ <- db.run(table.hInsert(data2))
          _ <- db.run(table.hInsert(data3))
          _ <- db.run(table.hInsert(data4))
          _ <- db.run(table.hDelete(_.dataId === data1.dataId))
          allVersions <- db.run(table.result)
          actualBeforeDeletion <- db.run(table.filter(_.isActual).result)
          deleted <- db.run(table.hDelete(_.dataValue === data1.dataValue))
        } yield (allVersions, actualBeforeDeletion, deleted)

        whenReady(prepare(action)) { case (allVersions, actualBeforeDeletion, deleted) =>
          allVersions.length shouldBe 4
          actualBeforeDeletion.length shouldBe 3
          deleted should have size 2
          deleted shouldBe actualBeforeDeletion.filter(_.dataValue == data1.dataValue).map { v => (v.dataId, v.versionId) }
        }
      }
      "correctly set versionDeletedAt for an actual version, leave other fields unchanged" in new Setup {
        val data = HistoricalTestData.random()

        def action = for {
          inserted <- db.run(table.hInsert(data))
          _ <- db.run(table.hDelete(_.dataId === data.dataId))
          afterDeletion <- db.run(table.result)
        } yield (inserted, afterDeletion)

        whenReady(prepare(action)) { case (inserted, afterDeletion) =>
          afterDeletion should have size 1
          afterDeletion.head.copy(versionDeletedAt = inserted.versionDeletedAt) shouldBe inserted
        }
      }
    }

    "use hUpdate and hUpdateRow methods" should {
      "return actual version after update" in new Setup {
        val data = HistoricalTestData.random()
        val upd = data.copy(dataValue = "updated")

        def action = for {
          _ <- db.run(table.hInsert(data))
          updateResult1 <- db.run(table.hUpdateRow(upd))
          afterUpdate1 <- db.run(table.hSelectAll.result)
          updateResult2 <- db.run(table.hUpdate(_.dataId === data.dataId)((h: HistoricalTestData) => h.copy(dataValue = "upd2")))
          afterUpdate2 <- db.run(table.hSelectAll.result)
        } yield (updateResult1, afterUpdate1, updateResult2, afterUpdate2)

        whenReady(prepare(action)) { case (updateResult1, afterUpdate1, updateResult2, afterUpdate2) =>
          updateResult1.map(Seq(_)).getOrElse(Seq.empty) shouldBe afterUpdate1
          updateResult2 shouldBe afterUpdate2
        }
      }
      "do nothing if there is no actual versions corresponding the given condition" in new Setup {
        val data = HistoricalTestData.random()
        val upd = HistoricalTestData.random()

        def action = for {
          _ <- db.run(table.hInsert(data))
          beforeUpdate <- db.run(table.result)
          updateResult <- db.run(table.hUpdateRow(upd))
          afterUpdate <- db.run(table.result)
        } yield (beforeUpdate, afterUpdate, updateResult)

        whenReady(prepare(action)) { case (beforeUpdate, afterUpdate, updateResult) =>
          beforeUpdate shouldBe afterUpdate
          updateResult shouldBe empty
        }
      }
      "do nothing by default (we can override this behaviour for any historical class) if there is no changes for found data" in new Setup {
        val data = HistoricalTestData.random()
        val upd = data

        def action = for {
          _ <- db.run(table.hInsert(data))
          beforeUpdate <- db.run(table.result)
          updateResult <- db.run(table.hUpdateRow(upd))
          afterUpdate <- db.run(table.result)
        } yield (beforeUpdate, afterUpdate, updateResult)

        whenReady(prepare(action)) { case (beforeUpdate, afterUpdate, updateResult) =>
          beforeUpdate shouldBe afterUpdate
          updateResult shouldBe empty
        }
      }
      "for found data delete actual version and add new one, deleted timestamp for the old version must be the same as created for the new one" in new Setup {
        val data = HistoricalTestData.random()
        val upd = HistoricalTestData.random(dataId = data.dataId)

        def action = for {
          _ <- db.run(table.hInsert(data))
          updateResult <- db.run(table.hUpdateRow(upd))
          afterUpdate <- db.run(table.result)
        } yield (afterUpdate, updateResult)

        whenReady(prepare(action)) { case (afterUpdate, updateResult) =>
          val deletedVersion = afterUpdate.filterNot(_.isActual).head
          val actualVersion = afterUpdate.filter(_.isActual).head

          afterUpdate should have size 2
          updateResult shouldBe Some(actualVersion)
          deletedVersion.versionDeletedAt.get shouldBe actualVersion.versionCreatedAt
          deletedVersion.toData shouldBe data.toData
          actualVersion.toData shouldBe upd.toData
        }
      }
      "use custom conditions to search and modify data" in new Setup {
        val searchBy = "update me"
        val updater = (p: HistoricalTestData) => p.copy(dataValue = "updated")

        val data1 = HistoricalTestData.random(dataValue = "don't update me pls")
        val data2 = HistoricalTestData.random(dataValue = searchBy)
        val data3 = HistoricalTestData.random(dataValue = searchBy)

        def action = for {
          _ <- db.run(table.hInsert(data1))
          _ <- db.run(table.hInsert(data2))
          _ <- db.run(table.hInsert(data3))
          updateResult <- db.run(table.hUpdate(_.dataValue === searchBy)(updater))
        } yield updateResult

        whenReady(prepare(action)) { updateResult =>
          updateResult.map(_.toData) shouldBe Seq(data2, data3).map(updater).map(_.toData)
        }
      }
      "correctly perform concurrent update operations" in new SetupConcurrentUpdate(
        longUpdVal = "updated by the 1st update operation", quickUpdVal = "updated by the 2nd update operation") {

        val oneUpdater = (p: HistoricalTestData) => p.copy(dataValue = longUpdVal)
        val anotherUpdater = (p: HistoricalTestData) => p.copy(otherValue = Some(quickUpdVal))
        val data = HistoricalTestData.random()

        val longAction = new TimedAction[Seq[HistoricalTestData]](
          db.run(table.hUpdate(_.dataId === data.dataId)(oneUpdater))
        )

        val quickAction = new TimedAction[Seq[HistoricalTestData]](
          db.run(table.hUpdate(_.dataId === data.dataId)(anotherUpdater)), pgWaitForUpdateCompletionSec / 2
        )

        def action = for {
          _ <- db.run(table.hInsert(data))
          _ <- Future.sequence(Seq(longAction, quickAction).map(_.run()))
          result <- db.run(table.hSelectAll.result)
        } yield result

        whenReady(prepare(action)) { result =>
          result should have size 1
          result.head.toData shouldBe anotherUpdater(oneUpdater(data)).toData

          quickAction.getStartPoint should be > longAction.getStartPoint
          quickAction.getStartPoint should be < longAction.getFinishPoint
          quickAction.getFinishPoint should be >= longAction.getFinishPoint


          println(s"long action started at ${longAction.getStartPoint.get}")
          println(s"quick action started at ${quickAction.getStartPoint.get}")
          println(s"long action finished at ${longAction.getFinishPoint.get}")
          println(s"quick action finished at ${quickAction.getFinishPoint.get}")
        }
      }
      "leave unchanged existing values in historical fields (dataId, versionId, createdAt, deletedAt)" in new Setup {
        val inserted = "inserted"
        val updated1 = "updated1"
        val updated2 = "updated2"
        val badVersion = VersionId.random()
        val data = HistoricalTestData.random(dataValue = inserted)
        val badRow = data.copy(dataValue = updated1, versionDeletedAt = Some(VersionDt.nowUTC()))
        val badUpdater = (h: HistoricalTestData) => h.copy(
          dataId = DataId.natural("badId"),
          versionId = badVersion,
          versionCreatedAt = nowUTC().plusYears(1),
          dataValue = updated2
        )

        def action = for {
          inserted <- db.run(table.hInsert(data))
          updated1 <- db.run(table.hUpdateRow(badRow))
          updated2 <- db.run(table.hUpdate(_.dataId === data.dataId)(badUpdater))
          allRecords <- db.run(table.result).map { r => r.map { v => v.dataValue -> v }.toMap }
          results = Seq(inserted) ++ updated1.map(Seq(_)).getOrElse(Seq.empty) ++ updated2
        } yield (results.map { v => v.dataValue -> v }.toMap, allRecords)

        whenReady(prepare(action)) { case (results, allRecords) =>
          allRecords should have size 3
          results.size shouldBe allRecords.size
          allRecords.values.forall(_.dataId == data.dataId) shouldBe true
          results(inserted) shouldBe allRecords(inserted).copy(versionDeletedAt = None)
          results(updated1) shouldBe allRecords(updated1).copy(versionDeletedAt = None)
          results(updated2) shouldBe allRecords(updated2)
          allRecords(updated2).versionId should not be badVersion
          allRecords(updated2).versionCreatedAt.value should be <= VersionDt.nowUTC().value
        }
      }
    }


    "correctly perform concurrent update and delete operations" in new SetupConcurrentUpdate(
      longUpdVal = "updated by an update operation", quickUpdVal = "") {

      val updater = (p: HistoricalTestData) => p.copy(dataValue = longUpdVal)
      val data = HistoricalTestData.random()

      val longAction = new TimedAction[Seq[HistoricalTestData]](
        db.run(table.hUpdate(_.dataId === data.dataId)(updater))
      )

      val quickAction = new TimedAction[Seq[(DataId, VersionId)]](
        db.run(table.hDelete(_.dataId === data.dataId)), pgWaitForUpdateCompletionSec / 2
      )

      def action = for {
        _ <- db.run(table.hInsert(data))
        concurrentResults <- Future.sequence(Seq(longAction, quickAction).map(_.run()))
        finalResult <- db.run(table.hSelectAll.result)
      } yield (concurrentResults.flatten, finalResult)

      whenReady(prepare(action)) { case (concurrentResults, finalResult) =>
        val updated = concurrentResults.collect { case r: HistoricalTestData => r }.head
        val deleted = concurrentResults.collect { case (id: DataIdClass, v: VersionIdClass) => (id, v) }.head

        updated.toData shouldBe updater(data).toData
        updated.isActual shouldBe true
        (updated.dataId, updated.versionId) shouldBe deleted
        finalResult shouldBe empty

        quickAction.getStartPoint should be > longAction.getStartPoint
        quickAction.getStartPoint should be < longAction.getFinishPoint
        quickAction.getFinishPoint should be >= longAction.getFinishPoint

        println(s"long action started at ${longAction.getStartPoint.get}")
        println(s"quick action started at ${quickAction.getStartPoint.get}")
        println(s"long action finished at ${longAction.getFinishPoint.get}")
        println(s"quick action finished at ${quickAction.getFinishPoint.get}")
      }
    }


    "hSelect" should {
      "return only actual versions according a given condition" in new Setup {
        val searchBy = "search me"
        val data1 = HistoricalTestData.random()
        val data2 = HistoricalTestData.random(dataValue = searchBy)
        val data3 = HistoricalTestData.random(dataValue = searchBy)
        val data4 = HistoricalTestData.random(dataValue = searchBy)

        def action = for {
          _ <- db.run(table.hInsert(data1))
          _ <- db.run(table.hInsert(data2))
          _ <- db.run(table.hInsert(data3))
          _ <- db.run(table.hInsert(data4))
          _ <- db.run(table.hDelete(_.dataId === data4.dataId))
          select <- db.run(table.hSelect(_.dataValue === searchBy).result)
        } yield select

        whenReady(prepare(action)) { result =>
          result.map(_.toData) shouldBe Seq(data2, data3).map(_.toData)
        }
      }
    }


    "hSelectAll" should {
      "return all actual versions from a table" in new Setup {
        val data1 = HistoricalTestData.random()
        val data2 = HistoricalTestData.random()
        val data3 = HistoricalTestData.random()
        val data4 = HistoricalTestData.random()

        def action = for {
          _ <- db.run(table.hInsert(data1))
          _ <- db.run(table.hInsert(data2))
          _ <- db.run(table.hInsert(data3))
          _ <- db.run(table.hInsert(data4))
          _ <- db.run(table.hDelete(_.dataId === data4.dataId))
          select <- db.run(table.hSelectAll.result)
        } yield select

        whenReady(prepare(action)) { result =>
          result.map(_.toData) shouldBe Seq(data1, data2, data3).map(_.toData)
        }
      }
    }


    "hUpdateOrInsert" should {
      "execute hInsert action with a given data if there is no data with the same dataId" in new Setup {
        val data1 = HistoricalTestData.random()
        val data2 = HistoricalTestData.random()

        def action = for {
          _ <- db.run(table.hInsert(data1))
          _ <- db.run(table.hUpdateOrInsert(data2))
          select <- db.run(table.hSelectAll.result)
        } yield select

        whenReady(prepare(action)) { result =>
          result.map(_.toData) shouldBe Seq(data1.toData, data2.toData)
        }
      }
      "execute hUpdateRow action with a given data if there is a data with the same dataId" in new Setup {
        val data1 = HistoricalTestData.random()
        val data2 = HistoricalTestData.random(dataId = data1.dataId)

        def action = for {
          _ <- db.run(table.hInsert(data1))
          _ <- db.run(table.hUpdateOrInsert(data2))
          select <- db.run(table.hSelectAll.result)
        } yield select

        whenReady(prepare(action)) { result =>
          result.map(_.toData) shouldBe Seq(data2.toData)
        }
      }
    }

    "use columns with VersionDt type in where clause" should {
      "be able to use '>', '<', '>=', '<=' and 'between' operators for them" in new Setup {
        val data = HistoricalTestData.random()

        def action = for {
          dt <- db.run(table.hInsert(data).map(_.versionCreatedAt))
          less <- db.run(table.hSelect(_.versionCreatedAt < dt).result)
          lessEq <- db.run(table.hSelect(_.versionCreatedAt <= dt).result)
          more <- db.run(table.hSelect(_.versionCreatedAt > dt).result)
          moreEq <- db.run(table.hSelect(_.versionDeletedAt >= dt).result)
          between <- db.run(table.hSelect(_.versionCreatedAt between(dt.minusDays(1), dt)).result)
        } yield (less, lessEq, more, moreEq, between)

        whenReady(prepare(action)) { case (less, lessEq, more, moreEq, between) =>
          less shouldBe empty
          lessEq should not be empty
          more shouldBe empty
          moreEq shouldBe empty
          between should not be empty
        }
      }
    }


  }


  implicit override val patienceConfig = PatienceConfig(timeout = Span(50, Seconds), interval = Span(50, Millis))


  class SetupConcurrentUpdate(val longUpdVal: String, val quickUpdVal: String) extends Setup {

    def pgWaitForUpdateCompletionSec: Int = 4

    class TimedAction[T](action: => Future[T], delaySec: Int = 0) {
      private var startedAt: Option[LocalDateTime] = None
      private var finishedAt: Option[LocalDateTime] = None

      def run(): Future[T] = {
        Thread.sleep(delaySec.toLong * 1000)
        for {
          _ <- Future.successful(startedAt = Some(nowUTC()))
          result <- action
          _ <- Future.successful(finishedAt = Some(nowUTC()))
        } yield result
      }

      def getStartPoint: Option[LocalDateTime] = startedAt

      def getFinishPoint: Option[LocalDateTime] = finishedAt
    }

    override protected def othersSqlScripts() =
      s"""
         |create or replace function fnDelay() returns trigger as ${"$$"}
         |begin
         |  perform pg_sleep($pgWaitForUpdateCompletionSec);
         |  return null;
         |end;
         |${"$$"} language plpgsql;
         |
         |create trigger trg_${tableName}_update_delay
         |after update on $tableName
         |for each row
         |when ((NEW.other_value != '$quickUpdVal' or NEW.other_value is null) and NEW.value = '$longUpdVal')
         |execute procedure fnDelay();
       """.stripMargin
  }

  trait Setup extends HistoricalSql[TestData, HistoricalTestData, TestHistoricalTable] {

    val table = TableQuery[TestHistoricalTable]

    def prepare[R](a: => Future[R]): Future[R] =
      dropOrCreateTable().flatMap(_ => a)


    protected def othersSqlScripts(): String = ""

    protected def tableName = table.shaped.value.tableName

    private def createTable(tableName: String) = {
      table.schema.create.andThen(
        for {
          _ <- sql"alter table #$tableName add constraint PK_#$tableName primary key(id, version)".as[Int]
          _ <- sql"create unique index idx_#${tableName}_id on #$tableName(id) where deleted_at is null".as[Int]
          _ <- sql"#${othersSqlScripts()}".as[Int]
        } yield ()
      )
    }

    private def dropOrCreateTable() = {

      db.run(
        for {
          found <- sql"""select table_name from "information_schema"."tables" where table_name = $tableName""".as[String].headOption
          _ <- found match {
            case Some(_) =>
              table.schema.drop.andThen(
                createTable(tableName)
              )
            case None =>
              createTable(tableName)
          }
        } yield ()
      )
    }

  }

}


/*
* Test entity and historical table for it
* */
case class TestDataId(value: String) extends DataId {
  final override lazy val asDataId: DataId = DataId.natural(value)

  final override def baseValue: String = asDataId.baseValue
}

case class TestData(id: TestDataId, dataValue: String, otherValue: Option[String])

case class HistoricalTestData(versionCreatedAt: VersionDt,
                              versionDeletedAt: Option[VersionDt],
                              versionId: VersionId,
                              dataId: DataId,
                              dataValue: String,
                              otherValue: Option[String]) extends HistoricalData[TestData] {
  lazy val id = TestDataId(dataId.baseValue)

  override def toData: TestData = TestData(id, dataValue, otherValue)
}

object HistoricalTestData extends ((VersionDt, Option[VersionDt], VersionId, DataId, String, Option[String]) => HistoricalTestData) {
  def random(versionCreatedAt: VersionDt = VersionDt.nowUTC(),
             versionDeletedAt: Option[VersionDt] = None,
             versionId: VersionId = VersionId.random(),
             dataId: DataId = DataId.natural(RandomString.get()),
             dataValue: String = RandomString.get(),
             otherValue: Option[String] = None): HistoricalTestData =
    new HistoricalTestData(versionCreatedAt, versionDeletedAt, versionId, dataId, dataValue, otherValue)
}

class TestHistoricalTable(tag: Tag) extends HistoricalTable[TestData, HistoricalTestData](tag, "test_table") {
  def dataValue = column[String]("value")

  def otherValue = column[Option[String]]("other_value")

  def * = (versionCreatedAt, versionDeletedAt, versionId, dataId, dataValue, otherValue) <> (HistoricalTestData.tupled, HistoricalTestData.unapply)
}


object RandomString {
  def get(): String = {
    UUID.randomUUID().toString
  }
}

