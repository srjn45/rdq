// rdq-java-worker — engine + Postgres binding.
//
// Depends on the client artifact (design 05, OQ-1): "submit here, execute
// there". The worker pulls in the submit-side API; the client never depends
// back on the worker.

base {
    archivesName.set("rdq-java-worker")
}

dependencies {
    api(project(":client"))

    implementation(libs.slf4j)

    // SpotBugs annotations (compile-time only, CLASS retention): the SPI value
    // records legitimately hold the wire Envelope, which is effectively immutable
    // through its defensive accessors but which SpotBugs flags as mutable.
    compileOnly(libs.spotbugs.annotations)
    testCompileOnly(libs.spotbugs.annotations)

    // The PostgreSQL storage binding talks JDBC. The driver is used by the
    // binding at runtime; the compile surface is pure java.sql / javax.sql so
    // consumers can supply their own pooled DataSource.
    runtimeOnly(libs.postgresql)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    // The compliance suite runs the FROZEN T2.1 migrations against a real
    // Postgres in a testcontainer (design 02 §3): same schema, same claim
    // semantics. Tests reference the driver's PGSimpleDataSource directly.
    testImplementation(libs.postgresql)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.junit)
    testRuntimeOnly(libs.slf4j.simple)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
