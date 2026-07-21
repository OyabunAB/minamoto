import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.mavenPublish)
    alias(libs.plugins.signing)
    alias(libs.plugins.dokka)
    alias(libs.plugins.jmh)
}

dependencies {
    api(project(":minamoto-core"))
    api(project(":minamoto-pool"))
    implementation(libs.bundles.core.postgres)
    testImplementation(libs.bundles.test)
    testImplementation(libs.bundles.test.containers)

    // JMH benchmark competitors — jmh scope only, never included in the published jar
    jmhImplementation(libs.aelv)
    jmhImplementation(libs.bundles.bench.competitors)
    jmhImplementation(libs.testcontainers.postgresql)
    jmhImplementation(libs.logback.classic)
    jmhImplementation(libs.coroutines.core)
    jmh(libs.jmh.annprocess)
}

val signingKey: String? = System.getenv("GPG_SIGNING_KEY")
val signingPassword: String? = System.getenv("GPG_SIGNING_PASSWORD")
val isPublishable = !version.toString().endsWith("-SNAPSHOT")

kotlin {
    jvmToolchain(libs.versions.jvm.get().toInt())
}

val jvmTargetVersion = JvmTarget.fromTarget(libs.versions.jvm.get())
tasks.compileKotlin { compilerOptions { jvmTarget.set(jvmTargetVersion) } }
tasks.compileTestKotlin { compilerOptions { jvmTarget.set(jvmTargetVersion) } }

tasks.named("compileJmhKotlin") {
    (this as org.jetbrains.kotlin.gradle.tasks.KotlinCompile)
        .compilerOptions { jvmTarget.set(jvmTargetVersion) }
}

jmh {
    warmupIterations.set(3)
    warmup.set("5s")
    iterations.set(5)
    timeOnIteration.set("10s")
    fork.set(1)
    resultFormat.set("JSON")
    resultsFile.set(project.file("${project.layout.buildDirectory.get()}/reports/jmh/results.json"))
    jvmArgsAppend.add("-Dorg.slf4j.simpleLogger.log.org.openjdk.jmh=WARN")
    if (project.hasProperty("jmh.include")) {
        includes.add(project.property("jmh.include").toString())
    }
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("started", "passed", "skipped", "failed")
        showStandardStreams = true
    }
}

tasks.named<Jar>("jar") {
    manifest { attributes("Automatic-Module-Name" to "se.oyabun.minamoto.postgres") }
}

val sourcesJar = tasks.register<Jar>("sourcesJar") {
    archiveClassifier = "sources"
    from(sourceSets.main.map { it.allSource })
}

val javadocJar = tasks.register<Jar>("javadocJar") {
    dependsOn(tasks.dokkaGenerate)
    archiveClassifier = "javadoc"
    from(layout.buildDirectory.dir("dokka/html"))
}

if (isPublishable && signingKey != null) {
    signing {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications)
    }
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/OyabunAB/minamoto")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(sourcesJar)
            artifact(javadocJar)
            pom {
                name = project.name
                description = "PostgreSQL wire protocol implementation for the minamoto R2DBC driver."
                url = "https://github.com/OyabunAB/minamoto"
                licenses {
                    license {
                        name = "Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com/OyabunAB/minamoto.git"
                    developerConnection = "scm:git:ssh://github.com:OyabunAB/minamoto.git"
                    url = "https://github.com/OyabunAB/minamoto"
                }
                developers {
                    developer {
                        id = "dansun"
                        name = "Daniel Sundberg"
                        email = "daniel.sundberg@oyabun.se"
                    }
                }
            }
        }
    }
}
