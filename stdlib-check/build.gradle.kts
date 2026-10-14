plugins { kotlin("multiplatform") }

kotlin {
  js()

  sourceSets {
    jsMain {
      kotlin.srcDir("../src/commonMain/kotlin")
      dependencies {
        implementation(libs.compact.encoding)
        implementation(libs.kotlinx.coroutines.core)
      }
    }
  }
}
