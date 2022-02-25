plugins {
    scala
}

// TODO: We need to figure out one way to manage all version in one place
val scalaBinaryVersion = "2.12"

dependencies {
    // Get recommended versions from platform project
    api(platform(project(":bom")))

    // Declare dependencies, no version required
    api("org.apache.calcite:calcite-core")
    api("org.slf4j:slf4j-api")
    api("org.scala-lang:scala-library")
}

sourceSets {
    main {
        withConvention(ScalaSourceSet::class) {
            java {
                setSrcDirs(listOf<String>())
            }
            scala {
                setSrcDirs(listOf("src/main/java", "src/main/scala"))
            }
        }
    }
    test {
        withConvention(ScalaSourceSet::class) {
            java {
                setSrcDirs(listOf<String>())
            }
            scala {
                setSrcDirs(listOf("src/test/java", "src/test/scala"))
            }
        }
    }
}
