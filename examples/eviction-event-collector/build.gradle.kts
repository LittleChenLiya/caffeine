plugins {
  `java-library`
  application
}

application {
  mainClass = "com.github.benmanes.caffeine.examples.eviction.EvictionEventCollectorDemo"
}

dependencies {
  implementation(libs.caffeine)

  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)
}

testing.suites {
  named<JvmTestSuite>("test") {
    useJUnitJupiter()
  }
}
