package com.eternal_search.geoip_service.dto

case class GeoRegionTreeInfo(
	id: Long,
	level: GeoRegionLevel,
	code: Option[String],
	name: Option[String],
	parent: Option[GeoRegionTreeInfo]
)
