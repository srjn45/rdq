// rdq-java-example — runnable consumer example + T8.2 cross-language runner.
//
// Main sources hold CrossLangWorkerRunner (a thin direct-SPI subprocess used by
// the Go cross-language e2e test to prove Java can claim and complete a task
// from a shared Postgres).  Test sources hold RetryExampleTest (the T7.6 JUnit
// quickstart).  Neither contributes to the JaCoCo coverage gate.

plugins {
    application
}

base {
    archivesName.set("rdq-java-example")
}

application {
    mainClass.set("io.github.srjn45.rdq.example.CrossLangWorkerRunner")
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
