package com.eternal_search.geoip_service.service

import cats.effect.IO
import com.eternal_search.geoip_service.dto.GeoIpUpdateStatus

trait GeoIpUpdater {
	def launchUpdate: IO[Unit]
	
	def status: IO[GeoIpUpdateStatus]
}
