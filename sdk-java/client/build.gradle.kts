// rdq-java-client — submit-side API.
//
// The client artifact must be usable on its own to submit work, with NO
// dependency on the worker/engine (design 05, OQ-1). Do not add a dependency
// on project(":worker") here.

base {
    archivesName.set("rdq-java-client")
}

dependencies {
    implementation(libs.slf4j)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
