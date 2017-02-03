package com.snapswap.versioning

import com.opentable.db.postgres.embedded._
import slick.jdbc.PostgresProfile.api._

trait TestDb {
  private def instance = EmbeddedPostgres.start().getPostgresDatabase

  val db = Database.forDataSource(instance)
}
