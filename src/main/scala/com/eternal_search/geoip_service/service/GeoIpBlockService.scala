package com.eternal_search.geoip_service.service

import cats.effect.IO
import cats.free.Free
import cats.implicits.catsSyntaxApplicativeId
import com.eternal_search.geoip_service.Database
import com.eternal_search.geoip_service.model.{GeoIpBlock, GeoIpLocation, GeoIpTimezone}
import doobie.ConnectionIO
import doobie.free.connection
import fs2.Stream
import doobie.implicits._
import io.getquill.Query

class GeoIpBlockService(val db: Database) {
	import db.dc._
	
	private val blocks = quote(querySchema[GeoIpBlock]("geoip_blocks"))
	private val locations = quote(querySchema[GeoIpLocation]("geoip_locations"))
	private val timezones = quote(querySchema[GeoIpTimezone]("geoip_timezones"))
	
	private val between = quote {
		(value: String, start: String, stop: String) => infix"$value BETWEEN $start AND $stop".as[Boolean]
	}
	
	def clear(): ConnectionIO[Unit] = run(blocks.delete).map(_ => ())
	
	def insert(blockStream: Stream[ConnectionIO, GeoIpBlock]): Stream[ConnectionIO, GeoIpBlock] =
		blockStream
			.chunkN(1024)
			.map(chunk => run(
				liftQuery(chunk.toVector)
					.foreach(block => blocks.insert(block))
			).map(_ => chunk))
			.flatMap(Stream.eval)
			.flatMap(Stream.chunk)
	
	def find(address: String, localeCode: String): IO[Option[(GeoIpBlock, Seq[GeoIpLocation], Option[GeoIpTimezone])]] =
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
			.transact(db.xa)
}
