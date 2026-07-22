// rdq-java-worker — engine + Postgres binding.
//
// Depends on the client artifact (design 05, OQ-1): "submit here, execute
// there". The worker pulls in the submit-side API; the client never depends
// back on the worker.

plugins {
    `maven-publish`
    `signing`
}

base {
    archivesName.set("rdq-java-worker")
}

// The Postgres storage binding (io.github.srjn45.rdq.worker.postgres) is covered
// by Docker-based testcontainers tests that are skipped when Docker is unavailable.
// Exclude it from the coverage gate so the instruction ratio reflects engine/SPI/policy
// code — all of which is covered by unit tests.
val postgresExclude = listOf("io/github/srjn45/rdq/worker/postgres/**")

tasks.named<JacocoReport>("jacocoTestReport") {
    classDirectories.setFrom(
        classDirectories.files.map { tree -> fileTree(tree) { exclude(postgresExclude) } }
    )
}

tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
    classDirectories.setFrom(
        classDirectories.files.map { tree -> fileTree(tree) { exclude(postgresExclude) } }
    )
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

// ─── Maven publishing config (approval-gated — see .github/workflows/publish.yml) ───
java {
    withJavadocJar()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            groupId = "io.github.srjn45"
            artifactId = "rdq-worker"
            version = project.version.toString()

            pom {
                name.set("rdq-worker")
                description.set("rdq worker engine + PostgreSQL binding — claim loop, retry policy, failure classification, and Postgres storage")
                url.set("https://github.com/srjn45/rdq")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        id.set("srjn45")
                        name.set("Srajan Pathak")
                        url.set("https://github.com/srjn45")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/srjn45/rdq.git")
                    developerConnection.set("scm:git:ssh://github.com/srjn45/rdq.git")
                    url.set("https://github.com/srjn45/rdq/tree/main")
                }
            }
        }
    }

    repositories {
        // Maven Central (Sonatype OSSRH). Credentials and namespace
        // io.github.srjn45 are operator-provisioned (T0.2, approval-gated).
        // Configured here; the actual publish task only runs via the
        // manual workflow_dispatch in .github/workflows/publish.yml.
        maven {
            name = "MavenCentral"
            val releasesRepoUrl = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
            credentials {
                username = providers.environmentVariable("MAVEN_CENTRAL_USERNAME").orNull
                password = providers.environmentVariable("MAVEN_CENTRAL_PASSWORD").orNull
            }
        }
    }
}

// Sign only when the in-memory key is present (publish workflow injects it;
// normal CI builds skip signing so ./gradlew build never requires a key).
signing {
    val signingKey = providers.environmentVariable("MAVEN_SIGNING_KEY").orNull
    val signingPassword = providers.environmentVariable("MAVEN_SIGNING_PASSWORD").orNull
    if (signingKey != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    }
}
