plugins { kotlin("multiplatform") }

kotlin {
  js()

  sourceSets { jsMain { kotlin.srcDir("../src/commonMain/kotlin") } }
}
