package com.eternal_search.geoip_service

import java.time.Instant
import cats.implicits.{catsSyntaxApply, catsSyntaxEitherId}
import cats.syntax.semigroupk._
import cats.effect.{ContextShift, IO, SyncIO, Timer}
import com.eternal_search.geoip_service.dto._
import com.eternal_search.geoip_service.maxmind.{MaxMindDownloader, MaxMindParser}
import com.eternal_search.geoip_service.model.GeoIpLocation
import com.eternal_search.geoip_service.service.{GeoIpBlockService, LastUpdateService}
import sttp.tapir.server.http4s._
import org.http4s.HttpRoutes

import scala.util.matching.Regex

class GeoIpRoutes(
	private val geoIpBlockService: GeoIpBlockService,
	private val lastUpdateService: LastUpdateService,
	private val maxMindDownloader: MaxMindDownloader
)(
	implicit private val contextShift: ContextShift[IO],
	implicit private val timer: Timer[IO]
) {
	private val IPV4_PATTERN = new Regex("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$")
	private val IPV6_PATTERN = new Regex("(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))")
	
	private def parseAddress(address: String): Either[String, String] =
		address match {
			case IPV4_PATTERN() => MaxMindParser.parseIPv4(address).asRight
			case IPV6_PATTERN() => MaxMindParser.parseIPv6(address).asRight
			case _ => "Invalid IP address format".asLeft
		}
	
	private def buildLocationsTree(locations: Seq[GeoIpLocation]): Option[GeoRegionInfo] =
		locations.headOption.map { location =>
			GeoRegionInfo(
				id = location.id,
				level = GeoRegionLevel.withNameLowercaseOnly(location.level.replace("_", "")),
				code = location.code,
				name = location.name,
				parent = buildLocationsTree(locations.drop(1))
			)
		}
	
	val searchRoute: HttpRoutes[IO] = GeoIpApi.searchEndpoint.toRoutes { case (localeCode, address) =>
		IO.pure(parseAddress(address))
			.flatMap(_.map(
				geoIpBlockService.find(_, localeCode)
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
	
	val statusRoute: HttpRoutes[IO] = GeoIpApi.statusEndpoint.toRoutes(_ =>
		lastUpdateService.lastUpdatedAt().flatMap(lastUpdatedAt =>
			maxMindDownloader.status.map(status =>
				GeoIpStatus(
					lastUpdate = lastUpdatedAt,
					updateStatus = status
				).asRight[String]
			)
		)
	)

	val updateRoute: HttpRoutes[IO] = GeoIpApi.updateEndpoint.toRoutes(_ => {
		maxMindDownloader.downloadAndUpdateDatabase.runAsync(_ => IO.unit).flatMap(_ =>
			SyncIO.pure(().asRight[String])
		).to[IO]
	})
	
	val routes: HttpRoutes[IO] = searchRoute <+> statusRoute <+> updateRoute
}
