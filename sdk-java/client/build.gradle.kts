// rdq-java-client — submit-side API.
//
// The client artifact must be usable on its own to submit work, with NO
// dependency on the worker/engine (design 05, OQ-1). Do not add a dependency
// on project(":worker") here.

plugins {
    `maven-publish`
    `signing`
}

base {
    archivesName.set("rdq-java-client")
}

dependencies {
    // Jackson is exposed on the public API: the envelope model surfaces
    // com.fasterxml.jackson.databind.JsonNode for error.detail and preserved
    // unknown fields, so it must be an `api` dependency, not `implementation`.
    api(libs.jackson.databind)

    implementation(libs.slf4j)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
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
            artifactId = "rdq-client"
            version = project.version.toString()

            pom {
                name.set("rdq-client")
                description.set("rdq submit-side client — envelope model and task submission API (no engine dependency)")
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
