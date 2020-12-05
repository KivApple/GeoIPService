package com.eternal_search.geoip_service

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, IO}
import io.getquill.SnakeCase
import org.flywaydb.core.Flyway

import io.getquill.{idiom => _, _}
import doobie.quill.DoobieContext
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux

import scala.concurrent.ExecutionContext

class Database(config: Config.DatabaseConfig)(
	implicit val contextShift: ContextShift[IO],
	implicit val concurrentEffect: ConcurrentEffect[IO],
	implicit val executionContext: ExecutionContext
) {
	val dc = new DoobieContext.Postgres(SnakeCase)
	
	val xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
		"org.postgresql.Driver",
		config.url,
		null,
		null,
		Blocker.liftExecutionContext(executionContext)
	)
	
	def runMigrations(): Unit =
		Flyway.configure().dataSource(config.url, null, null).load().migrate()
}
