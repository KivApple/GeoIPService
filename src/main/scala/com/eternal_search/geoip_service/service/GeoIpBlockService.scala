package com.eternal_search.geoip_service.service

import cats.effect.IO
import com.eternal_search.geoip_service.Database
import com.eternal_search.geoip_service.model.{GeoIpBlock, GeoIpLocation, GeoIpTimezone}
import doobie.ConnectionIO
import fs2.Stream
import doobie.implicits._

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
			
	def find(address: String, localeCode: String): IO[Option[(GeoIpBlock, Option[GeoIpLocation], Option[GeoIpTimezone])]] =
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
			.transact(db.xa)
}
