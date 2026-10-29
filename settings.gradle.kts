pluginManagement {
  repositories {
    google()
    mavenCentral()
    gradlePluginPortal()
  }
}

dependencyResolutionManagement {
  repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
  repositories {
    google()
    mavenCentral()
    exclusiveContent {
      forRepository { maven("https://jitpack.io") }
      filter { includeGroupByRegex("com\\.github\\.holepunchto\\..*") }
    }
  }
}

rootProject.name = "bare-rpc-kotlin"
