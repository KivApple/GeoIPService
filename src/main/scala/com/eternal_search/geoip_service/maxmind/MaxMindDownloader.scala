package com.eternal_search.geoip_service.maxmind

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, IO, Sync, Timer}
import cats.implicits.catsSyntaxEitherId
import com.eternal_search.geoip_service.dto.GeoIpUpdateStatus
import com.eternal_search.geoip_service.service.{GeoIpStorage, GeoIpUpdater}
import com.eternal_search.geoip_service.Config
import fs2.Stream
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.{Request, Uri}
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path, Paths}
import scala.concurrent.ExecutionContext

class MaxMindDownloader[F[_] : Sync : ContextShift](
	private val storage: GeoIpStorage[F],
	config: Config.MaxMindDownloaderConfig,
	tempDir: String,
	private val downloadingFlag: Ref[IO, Boolean],
	private val parsingFlag: Ref[IO, Boolean]
)(
	implicit val contextShift: ContextShift[IO],
	implicit val concurrentEffect: ConcurrentEffect[IO],
	implicit val timer: Timer[IO],
	implicit val executionContext: ExecutionContext
) extends GeoIpUpdater {
	private val log = LoggerFactory.getLogger(getClass)
	
	private val downloadUri = (config.licenseKey match {
		case Some(licenseKey) => config.downloadUrl.map(_.replace("@", licenseKey))
		case None => config.downloadUrl
	}).map(Uri.unsafeFromString)
	private val blocker = Blocker.liftExecutionContext(executionContext)
	
	override def status: IO[GeoIpUpdateStatus] =
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
		Stream.eval(Ref[IO].of(0)).flatMap(byteCounter =>
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
	
	private def parseDataArchive(path: Path): IO[Either[Throwable, Unit]] = {
		parsingFlag.getAndSet(true)
			.flatMap { flag =>
				if (!flag) {
					storage.update { updater =>
						MaxMindParser.parseDataArchive(updater, path, blocker)
					}.flatMap(_ => IO {
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
				IO(Files.deleteIfExists(path)).flatMap(_ =>
					downloadUri.map(uri => downloadDatabaseArchive(uri, path).map(_.map(_ => path)))
						.getOrElse(throw new Exception("No database download URL specified"))
				) <* downloadingFlag.set(false)
			} else {
				IO.pure(Left(new Exception("Database is already downloading now")))
			}
		)
	}
	
	override def launchUpdate(binaryStream: Stream[IO, Byte]): IO[Either[Throwable, Unit]] =
		downloadingFlag.getAndSet(true).flatMap(flag =>
			if (!flag) {
				val path = Paths.get(tempDir, "archive.zip")
				IO(Files.deleteIfExists(path)).flatMap(_ =>
					storeStream(binaryStream.through(logStreamProgress), path).flatMap {
						case Left(err) => IO.pure(Left(err))
						case Right(_) => parseDataArchive(path).runAsync(_ => IO.unit).map(_ => Right()).to[IO]
					}
				) <* downloadingFlag.set(false)
			} else {
				IO.pure(Left(new Exception("Database is already downloading now")))
			}
		)
	
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
	
	override def launchUpdate: IO[Unit] = downloadAndUpdateDatabase.runAsync(_ => IO.unit).to[IO]
}

object MaxMindDownloader {
	def newInstance[F[_] : Sync : ContextShift](
		storage: GeoIpStorage[F],
		config: Config.MaxMindDownloaderConfig,
		tempDir: String
	)(implicit
		contextShift: ContextShift[IO],
		concurrentEffect: ConcurrentEffect[IO],
		timer: Timer[IO],
		executionContext: ExecutionContext
	): IO[MaxMindDownloader[F]] =
		Ref[IO].of(false).flatMap(flag1 =>
			Ref[IO].of(false).map(flag2 =>
				new MaxMindDownloader[F](storage, config, tempDir, flag1, flag2)
			)
		)
}
