// rdq-java-example — runnable consumer example for the Java SDK (T7.6).
//
// All code lives in test sources: no main classes → JaCoCo coverage gate is vacuous.
// The test is skipped automatically when Docker is unavailable via
// @Testcontainers(disabledWithoutDocker = true), matching the pattern used by
// the worker compliance suite and WorkerIntegrationTest.

base {
    archivesName.set("rdq-java-example")
}

dependencies {
    testImplementation(project(":worker"))

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.junit)

    // PostgreSQL JDBC driver: needed for PGSimpleDataSource in the test harness.
    // project(":worker") exposes the driver only at runtime, not compile-time.
    testImplementation(libs.postgresql)

    testCompileOnly(libs.spotbugs.annotations)

    testRuntimeOnly(libs.slf4j.simple)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
