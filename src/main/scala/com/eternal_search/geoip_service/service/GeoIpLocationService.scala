package com.eternal_search.geoip_service.service

import cats.effect.IO
import com.eternal_search.geoip_service.Database
import com.eternal_search.geoip_service.model.GeoIpLocation
import doobie.ConnectionIO
import doobie.implicits._
import fs2.Stream

class GeoIpLocationService(val db: Database) {
	import db.dc._
	
	private val locations = quote(querySchema[GeoIpLocation]("geoip_locations"))
	
	def beginUpdate(): ConnectionIO[Unit] =
		sql"SET CONSTRAINTS geoip_locations_parent_id_locale_code_fkey, geoip_locations_timezone_id_fkey DEFERRED"
			.update.run.map(_ => ())
	
	def clear(): ConnectionIO[Unit] = run(locations.delete).map(_ => ())
	
	def insert(locationStream: Stream[ConnectionIO, GeoIpLocation]): Stream[ConnectionIO, GeoIpLocation] =
		locationStream
			.chunkN(1024)
			.map(chunk => run(
				liftQuery(chunk.toVector)
					.foreach(location => locations.insert(location))
			).map(_ => chunk))
			.flatMap(Stream.eval)
			.flatMap(Stream.chunk)
	
	def find(localeCode: String, name: String, limit: Int): IO[Seq[GeoIpLocation]] =
		run(locations.filter(location =>
			location.localeCode == lift(localeCode) && location.name.exists(_.startsWith(lift(name)))
		).take(lift(limit))).transact(db.xa)
}
