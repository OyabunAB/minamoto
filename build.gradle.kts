import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.dokka) apply false
    alias(libs.plugins.nexusPublish)
}

val projectVersion = System.getenv("VERSION") ?: "0.0.0-SNAPSHOT"
val isRelease = Regex("""^\d+\.\d+\.\d+$""").matches(projectVersion)

allprojects {
    group = "se.oyabun"
    version = projectVersion
}

if (isRelease) {
    nexusPublishing {
        repositories {
            sonatype {
                username.set(System.getenv("OSSRH_USERNAME"))
                password.set(System.getenv("OSSRH_PASSWORD"))
            }
        }
    }
}
