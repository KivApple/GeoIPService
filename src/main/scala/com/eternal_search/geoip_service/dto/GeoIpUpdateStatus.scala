package com.eternal_search.geoip_service.dto

import enumeratum._

sealed trait GeoIpUpdateStatus extends EnumEntry

object GeoIpUpdateStatus extends Enum[GeoIpUpdateStatus] with CirceEnum[GeoIpUpdateStatus] {
	case object Idle extends GeoIpUpdateStatus
	case object Downloading extends GeoIpUpdateStatus
	case object Parsing extends GeoIpUpdateStatus
	
	override def values: IndexedSeq[GeoIpUpdateStatus] = findValues
}
