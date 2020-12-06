package com.eternal_search.geoip_service

import sttp.tapir._
import sttp.tapir.generic.auto._
import sttp.tapir.json.circe._
import sttp.tapir.codec.enumeratum._
import io.circe.generic.auto._
import com.eternal_search.geoip_service.dto._

object GeoIpApi {
	val searchEndpoint: Endpoint[(String, String), String, GeoIpInfo, Any] =
		endpoint
			.get
			.in("search" / path[String]("localeCode") / path[String]("address"))
			.errorOut(stringBody)
			.out(jsonBody[GeoIpInfo])
	
	val localesEndpoint: Endpoint[Unit, String, Seq[String], Any] =
		endpoint
			.get
			.in("locales")
			.errorOut(stringBody)
			.out(jsonBody[Seq[String]])
	
	val statusEndpoint: Endpoint[Unit, String, GeoIpStatus, Any] =
		endpoint
			.get
			.in("status")
			.errorOut(stringBody)
			.out(jsonBody[GeoIpStatus])
	
	val updateEndpoint: Endpoint[Unit, String, Unit, Any] =
		endpoint
			.post
			.in("update")
			.errorOut(stringBody)
	
	val endpoints = Seq(searchEndpoint, localesEndpoint, statusEndpoint, updateEndpoint)
}
