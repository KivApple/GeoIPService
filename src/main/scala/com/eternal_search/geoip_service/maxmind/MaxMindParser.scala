package com.eternal_search.geoip_service.maxmind

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits.catsSyntaxApplicativeId
import com.eternal_search.geoip_service.model.{GeoIpBlock, GeoIpLocation, GeoIpTimezone}
import com.eternal_search.geoip_service.service.GeoIpStorage
import fs2.Stream
import org.slf4j.LoggerFactory

import java.io.{BufferedInputStream, InputStream}
import java.nio.file.{Files, Path}
import java.util.zip.ZipInputStream
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
	
	private def parseLocationsData[F[_], R](
		localeCode: String,
		stream: Stream[F, (Array[String], Map[String, Int])],
		updater: GeoIpStorage.Updater[F]
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
							.through(updater.insertTimezones)
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
			.through(updater.insertLocations)
			.fold(0) { case (count, _) => count + 1 }
			.map(log.info("Imported {} locations for locale {}", _, localeCode))
	
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
		makeIPRange(GeoIpStorage.parseIPv6(address), size.toInt)
	}
	
	private def makeIPv4Range(network: String): (String, String) = {
		val Array(address, size) = network.split('/')
		makeIPRange(GeoIpStorage.parseIPv4(address), size.toInt + 96)
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
			accuracyRadius = Some(entry(headers("accuracy_radius"))).filter(_.nonEmpty).flatMap(_.toIntOption),
			isAnonymousProxy = entry(headers("is_anonymous_proxy")) != "0",
			isSatelliteProvider = entry(headers("is_satellite_provider")) != "0"
		)
	}
	
	private def parseBlocksData[F[_], R](
		addressFamily: String,
		stream: Stream[F, (Array[String], Map[String, Int])],
		updater: GeoIpStorage.Updater[F]
	): Stream[F, Unit] = stream
		.filter { case (entry, headers) => entry.length == headers.size }
		.map { case (entry, headers) => parseBlockData(entry, headers, addressFamily) }
		.through(updater.insertBlocks)
		.fold(0) { case (count, _) => count + 1 }
		.map(log.info("Imported {} {} blocks", _, addressFamily))
	
	private def parseDataFile[F[_]](
		name: String,
		kind: String,
		stream: Stream[F, Byte],
		updater: GeoIpStorage.Updater[F]
	): Stream[F, Unit] = {
		log.debug("Loading data for {} ({})...", name, kind)
		stream
			.through(fs2.text.utf8Decode)
			.through(fs2.text.lines)
			.map(_.split(','))
			.mapAccumulate(Map[String, Int]()) {
				case (headers, line) => if (headers.isEmpty)
					(line.zipWithIndex.toMap, None)
				else
					(headers, Some(line))
			}
			.map(_.swap)
			.filter { case (entry, _) => entry.nonEmpty }
			.map { case (entry, headers) => (entry.get, headers) }
			.through { lines =>
				name match {
					case "Locations" => parseLocationsData(kind, lines, updater)
					case "Blocks" => parseBlocksData(kind, lines, updater)
					case _ => Stream(log.warn("Unknown data file type: {}", name))
				}
			}
	}
	
	def parseDataArchive[F[_] : Sync : ContextShift](
		updater: GeoIpStorage.Updater[F], path: Path, blocker: Blocker
	): F[Unit] = {
		Stream.resource(Resource.fromAutoCloseable(
			Sync[F].delay(new ZipInputStream(new BufferedInputStream(Files.newInputStream(path))))
		))
			.flatMap(Stream.unfoldEval(_)(zip =>
				Sync[F].delay(Option(zip.getNextEntry).map(entry => ((entry, zip), zip)))
			))
			.map { case (entry, zip) => (entry.getName, zip) }
			.filter { case (name, _) => name.endsWith(".csv")}
			.map { case (name, zip) => (name.split('/').last, zip) }
			.map { case (name, zip) => (String.join(".", name.split('.').dropRight(1):_*), zip) }
			.map { case (name, zip) => (name.split("-", 4).drop(2), zip) }
			.map { case (name, zip) => (
				name,
				fs2.io.readInputStream[F](
					zip.asInstanceOf[InputStream].pure[F],
					4096,
					blocker,
					closeAfterUse = false
				)
			) }
			.flatMap { case (name, stream) =>
				parseDataFile(name(0), name(1), stream, updater).flatMap(_ =>
					if (name(0) == "Locations")
						Stream.emit(name(1))
					else
						Stream.empty
				)
			}
			.through(updater.insertLocales)
			.fold(0) { case (count, _) => count + 1 }
			.map(log.info("Imported {} locales", _))
			.compile
			.drain
	}
}
