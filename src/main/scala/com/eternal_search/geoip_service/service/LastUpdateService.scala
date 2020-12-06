package com.eternal_search.geoip_service.service

import cats.effect.IO
import cats.implicits.catsSyntaxApplicativeId
import com.eternal_search.geoip_service.Database
import com.eternal_search.geoip_service.model.GeoIpUpdate
import doobie.ConnectionIO
import doobie.implicits._

import java.time.Instant
import java.util.Date

class LastUpdateService(val db: Database) {
	import db.dc._
	
	private val updates = quote(querySchema[GeoIpUpdate]("geoip_updates"))
	
	def markUpdated(): ConnectionIO[Unit] = run(updates.insert(lift(GeoIpUpdate(
		id = 0,
		updatedAt = new Date()
	))).returningGenerated(_.id)).map(_ => ())
	
	def lastUpdatedAt(): IO[Option[Instant]] = run(
		updates.map(_.id).max
	).flatMap {
		case Some(id) => run(updates.filter(_.id == lift(id)).map(_.updatedAt).take(1)).map(_.headOption)
		case None => Option[Date](null).pure[ConnectionIO]
	} .transact(db.xa).map(_.map(_.toInstant))
}
