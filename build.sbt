name := "geoip-service"

version := "0.1"

scalaVersion := "2.13.4"

libraryDependencies ++= Seq(
	"com.beachape" %% "enumeratum" % "1.6.1",
	"com.beachape" %% "enumeratum-circe" % "1.6.1",
	"io.circe" %% "circe-core" % "0.12.3",
	"io.circe" %% "circe-generic" % "0.12.3",
	"io.circe" %% "circe-parser" % "0.12.3",
	"io.circe" %% "circe-yaml" % "0.12.0",
	"com.softwaremill.sttp.tapir" %% "tapir-core" % "0.17.0-M9",
	"com.softwaremill.sttp.tapir" %% "tapir-enumeratum" % "0.17.0-M9",
	"com.softwaremill.sttp.tapir" %% "tapir-json-circe" % "0.17.0-M9",
	"com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % "0.17.0-M9",
	"com.softwaremill.sttp.tapir" %% "tapir-openapi-circe-yaml" % "0.17.0-M9",
	"com.softwaremill.sttp.tapir" %% "tapir-http4s-server" % "0.17.0-M9",
	"com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-http4s" % "0.17.0-M2",
	"ch.qos.logback" % "logback-classic" % "1.2.3",
	"com.github.pureconfig" %% "pureconfig" % "0.14.0",
	"co.fs2" %% "fs2-io" % "2.4.4",
	"org.http4s" %% "http4s-blaze-server" % "0.21.13", // Must be in sync with tapir-http4s-server version
	"org.http4s" %% "http4s-blaze-client" % "0.21.13",
	"org.tpolecat" %% "doobie-quill" % "0.9.0",
	"io.getquill" %% "quill-jdbc" % "3.5.3",
	"org.flywaydb" % "flyway-core" % "7.3.0",
	"org.postgresql" % "postgresql" % "42.2.18"
)
