import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
  alias(libs.plugins.kotlin.multiplatform)
  alias(libs.plugins.android.kotlin.multiplatform.library)
  alias(libs.plugins.ktfmt)
}

kotlin {
  jvmToolchain(21)
  explicitApi()

  @OptIn(org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation::class) abiValidation()

  compilerOptions {
    allWarningsAsErrors = true
    apiVersion = KotlinVersion.KOTLIN_2_2
    languageVersion = KotlinVersion.KOTLIN_2_2
  }

  jvm { compilerOptions { jvmTarget = JvmTarget.JVM_11 } }

  android {
    namespace = "to.holepunch.barerpc"
    compileSdk = 36
    minSdk = 29
    withHostTest {}
    compilerOptions { jvmTarget = JvmTarget.JVM_11 }
  }

  sourceSets { commonTest.dependencies { implementation(kotlin("test")) } }
}

ktfmt { googleStyle() }

val checkAscii =
  tasks.register("checkAscii") {
    val root = rootDir
    val files =
      fileTree(root) {
        include("**/*.kt", "**/*.kts", "**/*.md", "**/*.yml", "**/*.properties", "**/*.toml")
        exclude("**/build/**", "**/.gradle/**", "**/.kotlin/**")
      }
    inputs.files(files)
    doLast {
      val lines =
        files.files.flatMap { file ->
          file.readLines().mapIndexedNotNull { i, line ->
            if (line.any { it.code > 0x7f }) "${file.relativeTo(root)}:${i + 1}" else null
          }
        }
      if (lines.isNotEmpty())
        throw GradleException("Non-ASCII characters at:\n" + lines.joinToString("\n"))
    }
  }

tasks.check { dependsOn(checkAscii) }
