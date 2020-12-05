package com.eternal_search.geoip_service.model

case class GeoIpBlock(
	start: String,
	stop: String,
	locationId: Option[Long],
	postalCode: Option[String],
	latitude: Option[Double],
	longitude: Option[Double],
	accuracyRadius: Option[Int],
	isAnonymousProxy: Boolean,
	isSatelliteProvider: Boolean
)
