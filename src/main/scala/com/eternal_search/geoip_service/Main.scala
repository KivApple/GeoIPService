package com.eternal_search.geoip_service

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.semigroupk._
import com.eternal_search.geoip_service.maxmind.MaxMindDownloader
import com.eternal_search.geoip_service.service.{GeoIpBlockService, GeoIpLocationService, GeoIpTimezoneService}
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.syntax.kleisli._
import pureconfig.ConfigSource
import sttp.tapir.docs.openapi._
import sttp.tapir.openapi.circe.yaml._
import sttp.tapir.swagger.http4s.SwaggerHttp4s
import pureconfig.generic.auto._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object Main extends IOApp {
	private implicit val executionContext: ExecutionContextExecutor = ExecutionContext.global
	
	override def run(args: List[String]): IO[ExitCode] = {
		val config = ConfigSource.default.loadOrThrow[Config]
		
		val database = new Database(config.database)
		database.runMigrations()
		val geoIpBlockService = new GeoIpBlockService(database)
		val geoIpLocationService = new GeoIpLocationService(database)
		val geoIpTimezoneService = new GeoIpTimezoneService(database)
		
		val maxMindDownloader = new MaxMindDownloader(
			database,
			geoIpBlockService,
			geoIpLocationService,
			geoIpTimezoneService,
			config.maxMind,
			config.tempDir
		)
		//maxMindDownloader.downloadAndUpdateDatabase.unsafeRunSync()

		val geoIpRoutes = new GeoIpRoutes(geoIpBlockService).routes
		val swaggerRoutes = new SwaggerHttp4s(
			GeoIpApi.endpoints.toOpenAPI("GeoIP service", "1.0.0").toYaml
		).routes
		
		val startServer = BlazeServerBuilder[IO](ExecutionContext.global)
			.bindHttp(port = config.port, host = config.host)
			.withHttpApp((geoIpRoutes <+> swaggerRoutes).orNotFound)
			.serve
			.compile
			.drain
		
		(/* maxMindDownloader.downloadAndUpdateDatabase *> */ startServer).map(_ => ExitCode.Success)
	}
}
