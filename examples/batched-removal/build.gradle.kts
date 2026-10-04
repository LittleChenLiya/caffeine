plugins {
  `java-library`
}

dependencies {
  implementation("com.github.ben-manes.caffeine:caffeine:3.3.0")

  testImplementation("org.junit.jupiter:junit-jupiter:5.12.2")
}

testing.suites {
  named<JvmTestSuite>("test") {
    useJUnitJupiter("5.12.2")
  }
}
