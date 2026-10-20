import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
  alias(libs.plugins.kotlin.multiplatform)
  alias(libs.plugins.android.kotlin.multiplatform.library)
  alias(libs.plugins.dokka)
  alias(libs.plugins.ktfmt)
  id("maven-publish")
}

group = "com.github.holepunchto"

version = System.getenv("VERSION") ?: "0.0.0"

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

  sourceSets {
    commonMain.dependencies {
      implementation(libs.compact.encoding)
      api(libs.kotlinx.coroutines.core)
    }
    commonTest.dependencies {
      implementation(kotlin("test"))
      implementation(libs.kotlinx.coroutines.test)
    }
    jvmTest.dependencies { implementation(libs.kotlinx.serialization.json) }
  }
}

dokka {
  dokkaSourceSets.configureEach { reportUndocumented = true }
  dokkaPublications.html { failOnWarning = true }
}

ktfmt { googleStyle() }

val fixturesVersion = "0.1.0"

val npm =
  if (System.getProperty("os.name").startsWith("Windows")) listOf("cmd", "/c", "npm")
  else listOf("npm")

val fixturesDir = layout.buildDirectory.dir("bare-rpc-test")

val packFixtures =
  tasks.register<Exec>("packFixtures") {
    inputs.property("version", fixturesVersion)
    outputs.file(fixturesDir.map { it.file("bare-rpc-test-$fixturesVersion.tgz") })
    workingDir(fixturesDir)
    commandLine(npm + listOf("pack", "bare-rpc-test@$fixturesVersion", "--loglevel=error"))
  }

val unpackFixtures =
  tasks.register<Sync>("unpackFixtures") {
    from(packFixtures.map { tarTree(it.outputs.files.singleFile) })
    into(fixturesDir.map { it.dir("unpacked") })
  }

val peerDir = layout.buildDirectory.dir("peer")

val copyPeer =
  tasks.register<Sync>("copyPeer") {
    from("src/jvmTest/js")
    into(peerDir)
    preserve { include("node_modules/**") }
  }

val installPeer =
  tasks.register<Exec>("installPeer") {
    dependsOn(copyPeer)
    inputs.file("src/jvmTest/js/package.json")
    outputs.dir(peerDir.map { it.dir("node_modules") })
    workingDir(peerDir)
    commandLine(npm + listOf("install", "--no-package-lock", "--loglevel=error"))
  }

tasks.named<Test>("jvmTest") {
  useJUnitPlatform()
  inputs.files(unpackFixtures)
  inputs.files(copyPeer, installPeer)
  val peerScript = peerDir.map { it.file("peer.js").asFile.path }
  jvmArgumentProviders.add(
    CommandLineArgumentProvider { listOf("-Dbare-rpc.peer=${peerScript.get()}") }
  )
  val fixtures = fixturesDir.map { it.dir("unpacked/package/fixtures").asFile.path }
  jvmArgumentProviders.add(
    CommandLineArgumentProvider { listOf("-Dbare-rpc-test.fixtures=${fixtures.get()}") }
  )
}

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

tasks.check { dependsOn(checkAscii, "dokkaGenerate") }
