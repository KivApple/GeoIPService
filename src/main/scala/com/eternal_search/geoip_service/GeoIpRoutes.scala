package com.eternal_search.geoip_service

import cats.implicits.catsSyntaxEitherId
import cats.syntax.semigroupk._
import cats.effect.{ContextShift, IO, Timer}
import com.eternal_search.geoip_service.dto._
import com.eternal_search.geoip_service.model.GeoIpLocation
import com.eternal_search.geoip_service.service.{GeoIpStorage, GeoIpUpdater}
import sttp.tapir.server.http4s._
import org.http4s.HttpRoutes

import scala.util.matching.Regex

class GeoIpRoutes[F[_]](
	private val geoIpStorage: GeoIpStorage[F],
	private val geoIpUpdater: GeoIpUpdater
)(
	implicit private val contextShift: ContextShift[IO],
	implicit private val timer: Timer[IO]
) {
	private val IPV4_PATTERN = new Regex("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$")
	private val IPV6_PATTERN = new Regex("(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))")
	
	private def parseAddress(address: String): Either[String, String] =
		address match {
			case IPV4_PATTERN() => GeoIpStorage.parseIPv4(address).asRight
			case IPV6_PATTERN() => GeoIpStorage.parseIPv6(address).asRight
			case _ => "Invalid IP address format".asLeft
		}
	
	private def buildLocationsTree(locations: Seq[GeoIpLocation]): Option[GeoRegionTreeInfo] =
		locations.headOption.map { location =>
			GeoRegionTreeInfo(
				id = location.id,
				level = GeoRegionLevel.withNameLowercaseOnly(location.level.replace("_", "")),
				code = location.code,
				name = location.name,
				parent = buildLocationsTree(locations.drop(1))
			)
		}
	
	val searchIpRoute: HttpRoutes[IO] = GeoIpApi.searchIpEndpoint.toRoutes { case (localeCode, address) =>
		IO.pure(parseAddress(address))
			.flatMap(_.map(
				geoIpStorage.findAddress(_, localeCode)
					.map(_.map { case (block, locations, timezone) =>
						GeoIpInfo(
							ipAddress = address,
							isAnonymousProxy = block.isAnonymousProxy,
							isSatelliteProvider = block.isSatelliteProvider,
							postalCode = block.postalCode,
							latitude = block.latitude,
							longitude = block.longitude,
							accuracyRadius = block.accuracyRadius,
							timezone = timezone.map(_.name),
							isInEuropeanUnion = locations.headOption.flatMap(_.isInEuropeanUnion),
							location = buildLocationsTree(locations)
						)
					}.toRight("Location not found"))
			) match {
				case Left(err) => IO.pure(err.asLeft[GeoIpInfo])
				case Right(value) => value
			})
	}
	
	val searchLocationRoute: HttpRoutes[IO] = GeoIpApi.searchLocationEndpoint.toRoutes { case (localeCode, name) =>
		geoIpStorage.findLocations(localeCode, name, 10).map(_.map(location =>
			GeoRegionInfo(
				id = location.id,
				level = GeoRegionLevel.withNameLowercaseOnly(location.level.replace("_", "")),
				name = location.name,
				code = location.code,
				parentId = location.parentId
			)
		).asRight[String])
	}
	
	val localesRoute: HttpRoutes[IO] = GeoIpApi.localesEndpoint.toRoutes(_ =>
		geoIpStorage.findLocales().map(_.asRight[String])
	)
	
	val statusRoute: HttpRoutes[IO] = GeoIpApi.statusEndpoint.toRoutes(_ =>
		geoIpStorage.lastUpdatedAt().flatMap(lastUpdatedAt =>
			geoIpUpdater.status.map(status =>
				GeoIpStatus(
					lastUpdate = lastUpdatedAt,
					updateStatus = status
				).asRight[String]
			)
		)
	)

	val updateRoute: HttpRoutes[IO] = GeoIpApi.updateEndpoint.toRoutes(_ => {
		geoIpUpdater.launchUpdate.flatMap(_ =>
			IO.pure(().asRight[String])
		)
	})
	
	val routes: HttpRoutes[IO] = searchIpRoute <+> searchLocationRoute <+> localesRoute <+> statusRoute <+> updateRoute
}
