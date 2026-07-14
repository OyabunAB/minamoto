import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.mavenPublish)
    alias(libs.plugins.signing)
    alias(libs.plugins.dokka)
}

dependencies {
    api(project(":minamoto-core"))
    implementation(libs.bundles.core.postgres)
    testImplementation(libs.bundles.test)
    testImplementation(libs.bundles.test.containers)
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
