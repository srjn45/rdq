// rdq-java-example — runnable consumer example + T8.2 cross-language runner.
//
// Main sources hold CrossLangWorkerRunner: the T8.2 flagship subprocess that
// drives the REAL Java Worker engine (Worker.java + PostgresStorage) against a
// shared Postgres to claim and complete a redriven task, proving cross-language
// wire compatibility between the Go API/engine and the Java Worker.  Test
// sources hold RetryExampleTest (the T7.6 JUnit quickstart).  Neither
// contributes to the JaCoCo coverage gate.

plugins {
    application
}

base {
    archivesName.set("rdq-java-example")
}

application {
    mainClass.set("io.github.srjn45.rdq.example.CrossLangWorkerRunner")
}

// CrossLangWorkerRunner is an integration-test fixture (subprocess entry point),
// not library code with a unit-test coverage obligation.  Disable the JaCoCo
// coverage gate for this module; the 0.80 minimum still applies to :client and
// :worker (the published library modules).
tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
    enabled = false
}

dependencies {
    // Main sources: runner needs the worker + JDBC driver.
    implementation(project(":worker"))
    implementation(libs.postgresql)
    runtimeOnly(libs.slf4j.simple)

    // Test sources: JUnit quickstart.
    testImplementation(project(":worker"))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.postgresql)

    testCompileOnly(libs.spotbugs.annotations)

    testRuntimeOnly(libs.slf4j.simple)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
