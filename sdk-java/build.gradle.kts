import com.github.spotbugs.snom.Confidence
import com.github.spotbugs.snom.Effort
import com.github.spotbugs.snom.SpotBugsExtension
import com.github.spotbugs.snom.SpotBugsTask

plugins {
    id("com.diffplug.spotless") version "6.23.0" apply false
    id("com.github.spotbugs") version "6.1.11" apply false
    // maven-publish and signing are core Gradle plugins applied directly in
    // :client and :worker (not :example, which is not published).
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "com.github.spotbugs")
    apply(plugin = "jacoco")

    group = "io.github.srjn45"
    version = "2.1.0"

    repositories {
        mavenCentral()
    }

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
        withSourcesJar()
    }

    configure<SpotBugsExtension> {
        toolVersion.set("4.8.1")
        ignoreFailures.set(false)
        effort.set(Effort.MAX)
        reportLevel.set(Confidence.LOW)
    }

    tasks.withType<SpotBugsTask>().configureEach {
        reports {
            create("xml") {
                required.set(false)
            }
            create("html") {
                required.set(true)
                outputLocation.set(layout.buildDirectory.file("reports/spotbugs/spotbugs.html"))
            }
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    configure<JacocoPluginExtension> {
        toolVersion = "0.8.10"
    }

    tasks.named<JacocoReport>("jacocoTestReport") {
        dependsOn(tasks.named("test"))
        reports {
            xml.required.set(true)
            html.required.set(true)
            html.outputLocation.set(layout.buildDirectory.dir("jacocoHtml"))
        }
    }

    tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
        dependsOn(tasks.named("test"))
        violationRules {
            rule {
                limit {
                    minimum = 0.80.toBigDecimal()
                }
            }
        }
    }
}
