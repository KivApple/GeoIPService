package com.eternal_search.geoip_service.service

import cats.effect.IO
import com.eternal_search.geoip_service.Database
import com.eternal_search.geoip_service.model.GeoIpLocale
import doobie.ConnectionIO
import doobie.implicits._
import fs2.Stream

class GeoIpLocaleService(val db: Database) {
	import db.dc._
	
	private val locales = quote(querySchema[GeoIpLocale]("geoip_locales"))
	
	def clear(): ConnectionIO[Unit] = run(locales.delete).map(_ => ())
	
	def insert(stream: Stream[ConnectionIO, String]): Stream[ConnectionIO, String] =
		stream
			.chunkN(1024)
			.map(chunk =>
				run(
					liftQuery(chunk.toVector)
						.foreach(locale => locales.insert(GeoIpLocale(code = locale)))
				).map(_ => chunk)
			)
			.flatMap(Stream.eval)
			.flatMap(Stream.chunk)
	
	def findAll(): IO[Seq[String]] = run(locales.map(_.code)).transact(db.xa)
}
