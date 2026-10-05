plugins {
  `java-library`
}

dependencies {
  implementation(libs.caffeine)

  testImplementation(libs.jupiter)
  testImplementation(libs.truth)
}

testing.suites {
  named<JvmTestSuite>("test") {
    useJUnitJupiter()
  }
}
