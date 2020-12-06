package com.eternal_search.geoip_service.service

import cats.effect.{ContextShift, IO, Sync}
import com.eternal_search.geoip_service.model.{GeoIpBlock, GeoIpLocation, GeoIpTimezone}
import fs2.Stream

import java.time.Instant

trait GeoIpStorage[F[_]] {
	def update[R](updater: GeoIpStorage.Updater[F] => F[R]): IO[R]
	
	def findAddress(address: String, localeCode: String): IO[Option[(GeoIpBlock, Seq[GeoIpLocation], Option[GeoIpTimezone])]]
	
	def findLocations(localeCode: String, name: String, limit: Int): IO[Seq[GeoIpLocation]]
	
	def findLocales(): IO[Seq[String]]
	
	def lastUpdatedAt(): IO[Option[Instant]]
}

object GeoIpStorage {
	trait Updater[F[_]] {
		def insertBlocks(blocks: Stream[F, GeoIpBlock]): Stream[F, GeoIpBlock]
		
		def insertLocations(locations: Stream[F, GeoIpLocation]): Stream[F, GeoIpLocation]
		
		def insertTimezones(timezones: Stream[F, GeoIpTimezone]): Stream[F, GeoIpTimezone]
		
		def insertLocales(locales: Stream[F, String]): Stream[F, String]
	}
	
	def parseIPv6(address: String): String = {
		val parts = (address + "x").split("::")
		parts(parts.length - 1) = parts(parts.length - 1).dropRight(1)
		var words = parts(0).split(":").filter(_.nonEmpty).map(Integer.parseInt(_, 16))
		if (parts.size > 1) {
			val restWords = parts(1).split(":").filter(_.nonEmpty).map(Integer.parseInt(_, 16))
			words = words ++ Array.fill(8 - words.length - restWords.length)(0) ++ restWords
		}
		String.join("", words.map("%04x".format(_)): _*)
	}
	
	def parseIPv4(address: String): String = {
		val bytes = address.split('.').map(_.toInt)
		parseIPv6("::ffff:%02x%02x:%02x%02x".format(bytes(0), bytes(1), bytes(2), bytes(3)))
	}
}
