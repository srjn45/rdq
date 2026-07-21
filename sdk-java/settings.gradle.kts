/*
 * The settings file specifies the projects that make up the rdq Java SDK.
 *
 * The SDK is split into two published artifacts (OQ-1 / design 05):
 *   - io.github.srjn45:rdq-java-client — submit-side API
 *   - io.github.srjn45:rdq-java-worker — engine + Postgres binding (depends on client)
 *
 * See https://docs.gradle.org/8.13/userguide/multi_project_builds.html
 */

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    // Apply the foojay-resolver plugin to allow automatic download of JDKs
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.9.0"
}

rootProject.name = "rdq-java"

include("client", "worker")
