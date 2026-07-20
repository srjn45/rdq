plugins {
    `java-library`
    id("com.diffplug.spotless") version "6.23.0"
    id("com.github.spotbugs") version "6.1.11"
    jacoco
}

group = "com.github.srjn45"
version = "2.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.slf4j)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

spotbugs {
    toolVersion.set("4.8.1")
    ignoreFailures.set(false)
    effort.set(com.github.spotbugs.snom.Effort.MAX)
    reportLevel.set(com.github.spotbugs.snom.Confidence.LOW)  // <-- Here use enum, import required
}

tasks.withType<com.github.spotbugs.snom.SpotBugsTask>().configureEach {
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

tasks.named<Test>("test") {
    useJUnitPlatform()
}

jacoco {
    toolVersion = "0.8.10"
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required.set(true)
        html.required.set(true)
        html.outputLocation.set(layout.buildDirectory.dir("jacocoHtml"))
    }
}

val jacocoTestReport = tasks.named<JacocoReport>("jacocoTestReport").get()

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.test)

    violationRules {
        rule {
            limit {
                minimum = 0.80.toBigDecimal()
            }
        }
    }

    // Use the exact classDirs, sourceDirs, executionData from jacocoTestReport
    classDirectories = jacocoTestReport.classDirectories
    sourceDirectories = jacocoTestReport.sourceDirectories
    executionData = jacocoTestReport.executionData
}

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

artifacts {
    add("archives", sourcesJar.get())
}