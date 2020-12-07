package com.eternal_search.geoip_service.service

import cats.effect.IO
import com.eternal_search.geoip_service.dto.GeoIpUpdateStatus
import fs2.Stream

trait GeoIpUpdater {
	def launchUpdate: IO[Unit]
	
	def launchUpdate(binaryStream: Stream[IO, Byte]): IO[Either[Throwable ,Unit]]
	
	def status: IO[GeoIpUpdateStatus]
}
