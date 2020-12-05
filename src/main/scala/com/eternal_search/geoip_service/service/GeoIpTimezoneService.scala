package com.eternal_search.geoip_service.service

import com.eternal_search.geoip_service.Database
import com.eternal_search.geoip_service.model.GeoIpTimezone
import doobie.ConnectionIO
import fs2.Stream

class GeoIpTimezoneService(val db: Database) {
	import db.dc._
	
	private val timezones = quote(querySchema[GeoIpTimezone]("geoip_timezones"))
	
	def clear(): ConnectionIO[Unit] = run(timezones.delete).map(_ => ())
	
	def insert(timezoneStream: Stream[ConnectionIO, GeoIpTimezone]): Stream[ConnectionIO, GeoIpTimezone] =
		timezoneStream
			.chunkN(1024)
			.map(chunk => run(
				liftQuery(chunk.toVector)
					.foreach(timezone => timezones.insert(timezone).onConflictIgnore)
			).map(_ => chunk))
			.flatMap(Stream.eval)
			.flatMap(Stream.chunk)
}
