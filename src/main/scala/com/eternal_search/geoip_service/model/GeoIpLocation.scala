package com.eternal_search.geoip_service.model

case class GeoIpLocation(
	id: Long,
	localeCode: String,
	name: Option[String],
	code: Option[String],
	level: String,
	timezoneId: Option[Long],
	isInEuropeanUnion: Option[Boolean],
	parentId: Option[Long]
)
