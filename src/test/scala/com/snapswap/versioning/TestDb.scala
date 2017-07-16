package com.snapswap.versioning

import com.opentable.db.postgres.embedded._
import slick.jdbc.JdbcBackend


trait TestDb extends JdbcBackend {
  private def instance = EmbeddedPostgres.start().getPostgresDatabase

  val db = Database.forDataSource(instance, None)
}
