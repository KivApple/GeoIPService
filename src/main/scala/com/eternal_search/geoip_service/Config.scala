package com.eternal_search.geoip_service

import pureconfig.generic.auto._
import com.eternal_search.geoip_service.Config._

case class Config(
	host: String,
	port: Short,
	tempDir: String,
	database: DatabaseConfig,
	maxMind: MaxMindDownloaderConfig
)

object Config {
	case class DatabaseConfig(
		url: String
	)
	
	case class MaxMindDownloaderConfig(
		licenseKey: Option[String],
		downloadUrl: Option[String],
		updateIntervalDays: Option[Int]
	)
}
