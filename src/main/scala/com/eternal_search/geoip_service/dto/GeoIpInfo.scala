package com.eternal_search.geoip_service.dto

case class GeoIpInfo(
	ipAddress: String,
	isAnonymousProxy: Boolean,
	isSatelliteProvider: Boolean,
	postalCode: Option[String],
	latitude: Option[Double],
	longitude: Option[Double],
	accuracyRadius: Option[Int],
	timezone: Option[String],
	isInEuropeanUnion: Option[Boolean],
	location: Option[GeoRegionTreeInfo]
)
