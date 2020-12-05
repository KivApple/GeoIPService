package com.eternal_search.geoip_service.maxmind

import com.eternal_search.geoip_service.model.{GeoIpBlock, GeoIpLocation, GeoIpTimezone}
import fs2.Stream
import org.slf4j.LoggerFactory

import scala.collection.immutable.{Map, Seq}

object MaxMindParser {
	private val log = LoggerFactory.getLogger(getClass)
	private val MAX_IPV6_MASK = BigInt("ffffffffffffffffffffffffffffffff", 16)
	private val LEVELS = Seq(
		("continent", "code"),
		("country", "iso_code"),
		("subdivision_1", "iso_code"),
		("subdivision_2", "iso_code"),
		("city", ""),
		("", "metro")
	).map { case (key, codeSuffix) => (
		if (key.nonEmpty) key else codeSuffix,
		if (key.nonEmpty) Some("%s_name".format(key)) else None,
		if (codeSuffix.nonEmpty) {
			if (key.nonEmpty)
				Some("%s_%s".format(key, codeSuffix))
			else
				Some("%s_code".format(codeSuffix))
		} else
			None
	)
	}
	
	private case class LocationInfo(
		id: Seq[Long],
		name: Option[String],
		code: Option[String],
		level: String,
		timezone: Option[String],
		isInEuropeanUnion: Option[Boolean],
		parentKey: Option[String]
	)
	
	private def getLocationFromMap(
		path: Seq[(String, (String, Option[String], Option[String]))],
		locations: Map[String, LocationInfo]
	): (LocationInfo, Map[String, LocationInfo]) = path.last match {
		case (pathStr, (key, name, code)) => locations.get(pathStr) match {
			case Some(value) => (value, locations)
			case None =>
				val parentPath = path.dropRight(1)
				val parent = if (parentPath.nonEmpty) Some(getLocationFromMap(parentPath, locations)) else None
				val location = LocationInfo(
					id = Seq.empty,
					name = name,
					code = code,
					level = key,
					timezone = None,
					isInEuropeanUnion = None,
					parentKey = parentPath.lastOption.map(_._1)
				)
				(location, parent.map(_._2).getOrElse(locations).updated(pathStr, location))
		}
	}
	
	def parseLocationsData[F[_], R](
		localeCode: String,
		stream: Stream[F, (Array[String], Map[String, Int])],
		locationConsumer: Stream[F, GeoIpLocation] => Stream[F, R],
		timezoneConsumer: Stream[F, GeoIpTimezone] => Stream[F, R]
	): Stream[F, Unit] =
		stream
			.filter { case (entry, headers) => entry.length == headers.size }
			.mapAccumulate(Map[String, LocationInfo]()) { case (result, (entry, headers)) =>
				val path = Stream(LEVELS: _*)
					.map { case (key, nameKey, codeKey) => (key, nameKey.map(headers), codeKey.map(headers)) }
					.map { case (key, nameIndex, codeIndex) => (key, nameIndex.map(entry), codeIndex.map(entry)) }
					.map { case (key, name, code) => (
						key,
						name.flatMap(name => if (name.nonEmpty) Some(name) else None),
						code.flatMap(code => if (code.nonEmpty) Some(code) else None)
					)
					}
					.filter { case (_, name, code) => name.nonEmpty || code.nonEmpty }
					.map { case (key, name, code) => (key, name.map(_.replace("\"", "")), code) }
					.mapAccumulate("") { case (path, (key, name, code)) =>
						("%s/%s".format(
							path,
							"%s|%s".format(name.getOrElse(""), code.getOrElse(""))
						), (key, name, code))
					}
					.toVector
				val (location, newResult) = getLocationFromMap(path, result)
				path.last match {
					case (key, _) => (
						newResult.updated(key, location.copy(
							id = location.id ++ Seq(entry(headers("geoname_id")).toLong),
							timezone = Some(entry(headers("time_zone"))),
							isInEuropeanUnion = Some(entry(headers("is_in_european_union")) != "0")
						)), ()
					)
				}
			}
			.map { case (locationMap, _) => locationMap }
			.last
			.map(_.get)
			.map(locationMap =>
				locationMap.view.map { case (key, location) =>
					(
						key,
						if (location.id.isEmpty)
							location.copy(id = Seq(-(key.hashCode & 0x7FFFFFFF)))
						else
							location
					)
				}.toMap
			)
			.flatMap { locationMap =>
				Stream.unfold(locationMap.valuesIterator)(it => if (it.hasNext) Some(it.next, it) else None)
					.mapAccumulate(Map[String, Long]()) { case (timezoneMap, location) =>
						(
							location.timezone.map[Map[String, Long]](timezone =>
								timezoneMap
									.get(timezone)
									.map(_ => timezoneMap)
									.getOrElse(timezoneMap.updated(timezone, timezone.hashCode))
							).getOrElse(timezoneMap),
							location
						)
					}
					.map { case (timezoneMap, _) => timezoneMap }
					.last
					.map(_.get)
					.flatMap(timezoneMap =>
						Stream.unfold(timezoneMap.iterator)(it => if (it.hasNext) Some(it.next, it) else None)
							.map { case (name, id) => GeoIpTimezone(id = id, name = name) }
							.through(timezoneConsumer)
							.last
							.map(_ => timezoneMap)
					)
					.map(timezoneMap => (locationMap, timezoneMap))
			}
			.map { case (locationMap, timezoneMap) =>
				locationMap.view.flatMap { case (_, location) =>
					location.id.map(id => GeoIpLocation(
						id = id,
						localeCode = localeCode,
						name = location.name,
						code = location.code,
						level = location.level,
						timezoneId = location.timezone.map(timezoneMap),
						isInEuropeanUnion = location.isInEuropeanUnion,
						parentId = location.parentKey.map(locationMap).flatMap(_.id.headOption)
					))
				}
			}
			.flatMap(locations =>
				Stream.unfold(locations.iterator)(it => if (it.hasNext) Some(it.next, it) else None)
			)
			.through(locationConsumer)
			.fold(0) { case (count, _) => count + 1 }
			.map(log.info("Imported {} locations for locale {}", _, localeCode))
	
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
	
	private def makeIPRange(address: String, size: Int): (String, String) = {
		val invertedMask = (BigInt(1) << (128 - size)) - 1
		val mask = invertedMask ^ MAX_IPV6_MASK
		val net = BigInt(address, 16)
		val start = "%032x".format(net & mask)
		val stop = "%032x".format(net | invertedMask)
		assert(start.length == 32 && stop.length == 32)
		(start, stop)
	}
	
	private def makeIPv6Range(network: String): (String, String) = {
		val Array(address, size) = network.split('/')
		makeIPRange(parseIPv6(address), size.toInt)
	}
	
	private def makeIPv4Range(network: String): (String, String) = {
		val Array(address, size) = network.split('/')
		makeIPRange(parseIPv4(address), size.toInt + 96)
	}
	
	private def parseBlockData(
		entry: Array[String], headers: Map[String, Int], addressFamily: String
	): GeoIpBlock = {
		val (start, stop) = addressFamily match {
			case "IPv4" => makeIPv4Range(entry(headers("network")))
			case "IPv6" => makeIPv6Range(entry(headers("network")))
		}
		GeoIpBlock(
			start = start,
			stop = stop,
			locationId = Some(entry(headers("geoname_id"))).flatMap(_.toLongOption),
			postalCode = Some(entry(headers("postal_code"))).filter(_.nonEmpty),
			latitude = Some(entry(headers("latitude"))).filter(_.nonEmpty).flatMap(_.toDoubleOption),
			longitude = Some(entry(headers("longitude"))).filter(_.nonEmpty).flatMap(_.toDoubleOption),
			accuracyRadius = Some(entry(headers("accuracy_radius"))).filter(_.nonEmpty).flatMap(_.toFloatOption),
			isAnonymousProxy = entry(headers("is_anonymous_proxy")) != "0",
			isSatelliteProvider = entry(headers("is_satellite_provider")) != "0"
		)
	}
	
	def parseBlocksData[F[_], R](
		addressFamily: String,
		stream: Stream[F, (Array[String], Map[String, Int])],
		consumer: Stream[F, GeoIpBlock] => Stream[F, R]
	): Stream[F, Unit] = stream
		.filter { case (entry, headers) => entry.length == headers.size }
		.map { case (entry, headers) => parseBlockData(entry, headers, addressFamily) }
		.through(consumer)
		.fold(0) { case (count, _) => count + 1 }
		.map(log.info("Imported {} {} blocks", _, addressFamily))
}
