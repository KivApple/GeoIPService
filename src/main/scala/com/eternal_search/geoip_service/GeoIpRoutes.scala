package com.eternal_search.geoip_service

import java.time.Instant
import cats.implicits.catsSyntaxEitherId
import cats.syntax.semigroupk._
import cats.effect.{ContextShift, IO, Timer}
import com.eternal_search.geoip_service.dto._
import com.eternal_search.geoip_service.maxmind.MaxMindParser
import com.eternal_search.geoip_service.service.GeoIpBlockService
import sttp.tapir.server.http4s._
import org.http4s.HttpRoutes

class GeoIpRoutes(
	private val geoIpBlockService: GeoIpBlockService
)(
	implicit private val contextShift: ContextShift[IO],
	implicit private val timer: Timer[IO]
) {
	val searchRoute: HttpRoutes[IO] = GeoIpApi.searchEndpoint.toRoutes { case (localeCode, address) =>
		geoIpBlockService.find(
			if (address.matches("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$"))
				MaxMindParser.parseIPv4(address)
			else
				MaxMindParser.parseIPv6(address),
			localeCode
		).map(_.map { case (block, location, timezone) =>
			GeoIpInfo(
				ipAddress = address,
				isAnonymousProxy = block.isAnonymousProxy,
				isSatelliteProvider = block.isSatelliteProvider,
				postalCode = block.postalCode,
				latitude = block.latitude,
				longitude = block.longitude,
				accuracyRadius = block.accuracyRadius,
				timezone = timezone.map(_.name),
				isInEuropeanUnion = location.flatMap(_.isInEuropeanUnion),
				region = location.map(location => {
					GeoRegionInfo(
						id = location.id,
						level = GeoRegionLevel.withNameLowercaseOnly(location.level),
						code = location.code,
						name = location.name,
						parent = None // TODO
					)
				})
			)
		}.toRight("Location not found"))
	}
	
	val statusRoute: HttpRoutes[IO] = GeoIpApi.statusEndpoint.toRoutes(_ => IO(GeoIpStatus(
		lastUpdate = Instant.now()
	).asRight[String]))
	
	val routes: HttpRoutes[IO] = searchRoute <+> statusRoute
}
