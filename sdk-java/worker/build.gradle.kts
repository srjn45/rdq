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

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
