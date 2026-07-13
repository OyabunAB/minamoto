rootProject.name = "minamoto"

include("minamoto-core", "minamoto-pool", "minamoto-postgres")

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/OyabunAB/*")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN") ?: System.getenv("GHA_READ_PACKAGES_TOKEN")
            }
        }
    }
}
