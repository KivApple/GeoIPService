package com.eternal_search.geoip_service.maxmind

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, IO, Resource, Sync, Timer}
import cats.implicits.catsSyntaxApplicativeId
import com.eternal_search.geoip_service.dto.GeoIpUpdateStatus
import com.eternal_search.geoip_service.service.{GeoIpBlockService, GeoIpLocationService, GeoIpTimezoneService, LastUpdateService}
import com.eternal_search.geoip_service.{Config, Database}
import doobie.ConnectionIO
import fs2.Stream
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.{Request, Uri}
import org.slf4j.LoggerFactory

import java.io.{BufferedInputStream, InputStream}
import java.nio.file.{Files, Path, Paths}
import java.util.zip.ZipInputStream
import scala.concurrent.ExecutionContext

class MaxMindDownloader(
	private val database: Database,
	private val geoIpBlockService: GeoIpBlockService,
	private val geoIpLocationService: GeoIpLocationService,
	private val geoIpTimezoneService: GeoIpTimezoneService,
	private val lastUpdateService: LastUpdateService,
	config: Config.MaxMindDownloaderConfig,
	tempDir: String
)(
	implicit val contextShift: ContextShift[IO],
	implicit val concurrentEffect: ConcurrentEffect[IO],
	implicit val timer: Timer[IO],
	implicit val executionContext: ExecutionContext
) {
	private val log = LoggerFactory.getLogger(getClass)
	
	private val downloadUri = Uri.unsafeFromString(config.downloadUrl.replace("@", config.licenseKey))
	private val blocker = Blocker.liftExecutionContext(executionContext)
	private val downloadingFlag = Ref.of(false).unsafeRunSync()
	private val parsingFlag = Ref.of(false).unsafeRunSync()
	
	def status: IO[GeoIpUpdateStatus] =
		downloadingFlag.get.flatMap(downloadingFlag =>
			if (downloadingFlag)
				IO.pure(GeoIpUpdateStatus.Downloading)
			else
				parsingFlag.get.map(parsingFlag =>
					if (parsingFlag)
						GeoIpUpdateStatus.Parsing
					else
						GeoIpUpdateStatus.Idle
				)
		)
	
	private def streamUrl(uri: Uri): Stream[IO, Byte] =
		BlazeClientBuilder(executionContext)
			.stream
			.flatMap(_.stream(Request[IO](uri = uri)))
			.flatMap(_.body)
	
	private def logStreamProgress(stream: Stream[IO, Byte]): Stream[IO, Byte] =
		Stream.eval(Ref.of(0)).flatMap(byteCounter =>
			stream.chunks
				.flatMap(chunk =>
					Stream.eval(byteCounter.updateAndGet(_ + chunk.size))
						.flatMap(count => {
							log.debug(s"Received $count bytes")
							Stream.chunk(chunk)
						})
				)
		)
	
	private def storeStream(stream: Stream[IO, Byte], path: Path): IO[Either[Throwable, Unit]] =
		stream.through(fs2.io.file.writeAll(path, blocker))
			.compile
			.drain
			.attempt
	
	private def parseDataFile(name: String, kind: String, stream: Stream[ConnectionIO, Byte]): Stream[ConnectionIO, Unit] = {
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
					case "Locations" => MaxMindParser.parseLocationsData(
						kind, lines,
						geoIpLocationService.insert,
						geoIpTimezoneService.insert
					)
					case "Blocks" => MaxMindParser.parseBlocksData(kind, lines, geoIpBlockService.insert)
					case _ => Stream(log.warn("Unknown data file type: {}", name))
				}
			}
	}
	
	private def doParseDataArchive(path: Path): ConnectionIO[Unit] = {
		Stream.resource(Resource.fromAutoCloseable(
			Sync[ConnectionIO].delay(new ZipInputStream(new BufferedInputStream(Files.newInputStream(path))))
		))
			.flatMap(Stream.unfoldEval(_)(zip =>
				Sync[ConnectionIO].delay(Option(zip.getNextEntry).map(entry => ((entry, zip), zip)))
			))
			.map { case (entry, zip) => (entry.getName, zip) }
			.filter { case (name, _) => name.endsWith(".csv")}
			.map { case (name, zip) => (name.split('/').last, zip) }
			.map { case (name, zip) => (String.join(".", name.split('.').dropRight(1):_*), zip) }
			.map { case (name, zip) => (name.split("-", 4).drop(2), zip) }
			.map { case (name, zip) => (
				name,
				fs2.io.readInputStream[ConnectionIO](
					zip.asInstanceOf[InputStream].pure[ConnectionIO],
					4096,
					blocker,
					closeAfterUse = false
				)
			) }
			.flatMap { case (name, stream) => parseDataFile(name(0), name(1), stream) }
			.compile
			.drain
	}
	
	private def parseDataArchive(path: Path): IO[Either[Throwable, Unit]] = {
		import doobie.implicits._
		
		parsingFlag.getAndSet(true)
			.flatMap { flag =>
				if (!flag) {
					geoIpLocationService.beginUpdate()
						.flatMap(_ => geoIpLocationService.clear())
						.flatMap(_ => geoIpTimezoneService.clear())
						.flatMap(_ => geoIpBlockService.clear())
						.flatMap(_ => doParseDataArchive(path))
						.flatMap(_ => lastUpdateService.markUpdated())
						.transact(database.xa)
						.flatMap(_ => IO {
							Files.deleteIfExists(path)
							()
						})
						.attempt <* parsingFlag.set(false)
				} else {
					IO.pure(Left(new Exception("Database is already parsing now")))
				}
			}
	}
	
	private def downloadDatabaseArchive(uri: Uri, path: Path): IO[Either[Throwable, Unit]] = {
		log.info(s"Downloading archive $uri to $path...")
		storeStream(streamUrl(uri).through(logStreamProgress), path).flatMap {
			case result@Left(err) =>
				IO({
					log.error("Failed to download database", err)
					Files.deleteIfExists(path)
					result
				})
			case result@Right(_) =>
				IO.pure(result) <* IO(log.info("Database archive downloading is done"))
		}
	}
	
	def downloadArchive: IO[Either[Throwable, Path]] = {
		downloadingFlag.getAndSet(true).flatMap(flag =>
			if (!flag) {
				val path = Paths.get(tempDir, "archive.zip")
				IO(Files.exists(path)).flatMap(
					if (_) IO({
						log.info("Database archive already exists. Skipping downloading...")
						Right(path)
					}) else downloadDatabaseArchive(downloadUri, path).map(_.map(_ => path))
				) <* downloadingFlag.set(false)
			} else {
				IO.pure(Left(new Exception("Database is already downloading now")))
			}
		)
	}
	
	def downloadAndUpdateDatabase: IO[Either[Throwable, Unit]] =
		downloadArchive.flatMap {
			case Left(err) => IO.pure(Left(err))
			case Right(value) => parseDataArchive(value)
		}.flatMap {
			case Left(err) =>
				IO(log.error("Failed to update database", err)) *> IO.pure(Left(err))
			case Right(_) =>
				IO(Right(log.info("Database updated successfully")))
		}
}
