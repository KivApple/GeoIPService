package com.eternal_search.geoip_service.dto

import enumeratum._

sealed trait GeoRegionLevel extends EnumEntry

object GeoRegionLevel extends Enum[GeoRegionLevel] with CirceEnum[GeoRegionLevel] {
	case object Continent extends GeoRegionLevel
	case object Country extends GeoRegionLevel
	case object Subdivision1 extends GeoRegionLevel
	case object Subdivision2 extends GeoRegionLevel
	case object City extends GeoRegionLevel
	case object Metro extends GeoRegionLevel
	
	override val values: IndexedSeq[GeoRegionLevel] = findValues
}
