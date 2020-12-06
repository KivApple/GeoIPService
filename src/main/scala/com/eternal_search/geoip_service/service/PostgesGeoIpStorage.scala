package com.eternal_search.geoip_service.service

import cats.effect.{Blocker, ConcurrentEffect, ContextShift, IO}
import cats.implicits.catsSyntaxApplicativeId
import com.eternal_search.geoip_service.Config
import com.eternal_search.geoip_service.model.{GeoIpBlock, GeoIpLocale, GeoIpLocation, GeoIpTimezone, GeoIpUpdate}
import doobie.ConnectionIO
import doobie.quill.DoobieContext
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import io.getquill.{EntityQuery, Query, SnakeCase}
import org.flywaydb.core.Flyway

import scala.concurrent.ExecutionContext
import doobie.implicits._
import fs2.Stream

import java.time.Instant
import java.util.Date

class PostgesGeoIpStorage(config: Config.DatabaseConfig)(
	implicit val contextShift: ContextShift[IO],
	implicit val concurrentEffect: ConcurrentEffect[IO],
	implicit val executionContext: ExecutionContext
) extends GeoIpStorage[ConnectionIO] {
	private val dc = new DoobieContext.Postgres(SnakeCase)
	
	private val xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
		"org.postgresql.Driver",
		config.url,
		null,
		null,
		Blocker.liftExecutionContext(executionContext)
	)
	
	def runMigrations(): IO[Unit] =
		IO(Flyway.configure().dataSource(config.url, null, null).load().migrate())
	
	import dc._
	
	private val blocks = quote(querySchema[GeoIpBlock]("geoip_blocks"))
	private val locations = quote(querySchema[GeoIpLocation]("geoip_locations"))
	private val timezones = quote(querySchema[GeoIpTimezone]("geoip_timezones"))
	private val locales = quote(querySchema[GeoIpLocale]("geoip_locales"))
	private val updates = quote(querySchema[GeoIpUpdate]("geoip_updates"))
	
	private val between = quote {
		(value: String, start: String, stop: String) => infix"$value BETWEEN $start AND $stop".as[Boolean]
	}
	
	override def findLocales(): IO[Seq[String]] = run(locales.map(_.code)).transact(xa)
	
	override def findAddress(
		address: String, localeCode: String
	): IO[Option[(GeoIpBlock, Seq[GeoIpLocation], Option[GeoIpTimezone])]] =
		run(
			blocks
				.leftJoin(locations)
				.on((block, location) => block.locationId.contains(location.id) && (location.localeCode == lift(localeCode)))
				.leftJoin(timezones)
				.on { case ((_, location), timezone) => location.flatMap(_.timezoneId).contains(timezone.id) }
				.filter { case ((block, _), _) => between(lift(address), block.start, block.stop) }
				.take(1)
		)
			.map(_.headOption)
			.map(_.map { case ((block, location), timezone) => (block, location, timezone) })
			.flatMap {
				case Some((block, location, timezone)) =>
					location.map(_.id) match {
						case Some(id) => val t: ConnectionIO[Option[(GeoIpBlock, Seq[GeoIpLocation], Option[GeoIpTimezone])]] =
							run(infix"""
									WITH RECURSIVE parents AS (
										SELECT * FROM geoip_locations
											WHERE id = ${lift(id)} AND locale_code = ${lift(localeCode)}
										UNION SELECT p.* FROM geoip_locations p
											INNER JOIN parents c
												ON c.parent_id = p.id and c.locale_code = p.locale_code
									) SELECT
										id, locale_code as localeCode, name, code, level,
										timezone_id as timezoneId,
					 					is_in_european_union as isInEuropeanUnion,
					 					parent_id as parentId
									FROM parents
								""".as[Query[GeoIpLocation]]
							).map(locations =>
								Option((block, locations, timezone))
							)
							t
						case None =>
							val t: ConnectionIO[Option[(GeoIpBlock, Seq[GeoIpLocation], Option[GeoIpTimezone])]] =
								Option((block, location match {
									case Some(a) => Seq(a)
									case None => Seq.empty
								}, timezone)).pure[ConnectionIO]
							t
					}
				case None => val t: ConnectionIO[Option[(GeoIpBlock, Seq[GeoIpLocation], Option[GeoIpTimezone])]] =
					Option[(GeoIpBlock, Seq[GeoIpLocation], Option[GeoIpTimezone])](null).pure[ConnectionIO]
					t
			}
			.transact(xa)
	
	override def findLocations(localeCode: String, name: String, limit: Index): IO[Seq[GeoIpLocation]] =
		run(locations.filter(location =>
			location.localeCode == lift(localeCode) && location.name.exists(_.startsWith(lift(name)))
		).take(lift(limit))).transact(xa)
	
	override def lastUpdatedAt(): IO[Option[Instant]] = run(
		updates.map(_.id).max
	).flatMap {
		case Some(id) => run(updates.filter(_.id == lift(id)).map(_.updatedAt).take(1)).map(_.headOption)
		case None => Option[Date](null).pure[ConnectionIO]
	} .transact(xa).map(_.map(_.toInstant))
	
	private def createUpdater = new GeoIpStorage.Updater[ConnectionIO] {
		override def insertBlocks(stream: Stream[ConnectionIO, GeoIpBlock]): Stream[ConnectionIO, GeoIpBlock] =
			stream
				.chunkN(1024)
				.map(chunk => run(
					liftQuery(chunk.toVector)
						.foreach(block => blocks.insert(block))
				).map(_ => chunk))
				.flatMap(Stream.eval)
				.flatMap(Stream.chunk)
		
		override def insertLocations(stream: Stream[ConnectionIO, GeoIpLocation]): Stream[ConnectionIO, GeoIpLocation] =
			stream
				.chunkN(1024)
				.map(chunk => run(
					liftQuery(chunk.toVector)
						.foreach(block => locations.insert(block))
				).map(_ => chunk))
				.flatMap(Stream.eval)
				.flatMap(Stream.chunk)
		
		override def insertTimezones(stream: Stream[ConnectionIO, GeoIpTimezone]): Stream[ConnectionIO, GeoIpTimezone] =
			stream
				.chunkN(1024)
				.map(chunk => run(
					liftQuery(chunk.toVector)
						.foreach(block => timezones.insert(block))
				).map(_ => chunk))
				.flatMap(Stream.eval)
				.flatMap(Stream.chunk)
		
		override def insertLocales(stream: Stream[ConnectionIO, String]): Stream[ConnectionIO, String] =
			stream
				.chunkN(1024)
				.map(chunk => run(
					liftQuery(chunk.map(GeoIpLocale).toVector)
						.foreach(block => locales.insert(block))
				).map(_ => chunk))
				.flatMap(Stream.eval)
				.flatMap(Stream.chunk)
	}
	
	override def update[R](updater: GeoIpStorage.Updater[ConnectionIO] => ConnectionIO[R]): IO[R] =
		run(blocks.delete)
			.flatMap(_ => run(locations.delete))
			.flatMap(_ => run(timezones.delete))
			.flatMap(_ => run(locales.delete))
			.flatMap(_ => updater(createUpdater))
			.flatMap(result => run(updates.insert(lift(GeoIpUpdate(
				id = 0,
				updatedAt = new Date()
			))).returningGenerated(_.id)).map(_ => result))
			.transact(xa)
}
