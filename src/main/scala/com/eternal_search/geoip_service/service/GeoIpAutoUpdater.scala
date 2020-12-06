package com.eternal_search.geoip_service.service

import cats.effect.{IO, Timer}

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.DurationInt

object GeoIpAutoUpdater {
	def run[F[_]](storage: GeoIpStorage[F], updater: GeoIpUpdater, intervalDays: Int)(implicit timer: Timer[IO]): IO[Unit] = {
		def go(): IO[Unit] = storage.lastUpdatedAt().flatMap { lastUpdatedAt =>
			val delta = lastUpdatedAt.map(_.until(Instant.now(), ChronoUnit.DAYS)).getOrElse(Long.MaxValue)
			if (delta >= intervalDays) {
				updater.launchUpdate
			} else {
				IO.unit
			} *> IO.sleep(1.hour).flatMap(_ => go())
		}
		go().runAsync(_ => IO.unit).to[IO]
	}
}
