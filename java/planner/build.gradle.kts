import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    antlr
    kotlin("jvm")
}

// TODO: We need to figure out one way to manage all version in one place
val scalaBinaryVersion = "2.12"

dependencies {
    // Get recommended versions from platform project
    api(platform(project(":bom")))

    // Declare dependencies, no version required
    api("org.apache.calcite:calcite-core")
    api(project(":common"))
    api(project(":catalog"))
    api(project(":pgwire"))
    api(project(":planner2"))
    implementation(project(":proto"))
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    api("org.slf4j:slf4j-api")
    api("org.reflections:reflections")
    api("com.typesafe.akka:akka-actor-typed_${scalaBinaryVersion}")
    api("com.google.inject:guice")
    runtimeOnly("ch.qos.logback:logback-classic")
    api("com.google.protobuf:protobuf-java-util")
    api("org.apache.commons:commons-lang3")
    api("org.scala-lang:scala-library")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.apache.calcite:calcite-server")
    testImplementation("org.mockito:mockito-core")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("com.pholser:junit-quickcheck-core")
    testImplementation("org.hamcrest:hamcrest-all")
    // TODO: Manage all dependency versions in one place.
    antlr("org.antlr:antlr4:4.9.2")
}

tasks.generateGrammarSource {
    arguments = arguments + listOf("-visitor")
    outputDirectory = File("build/generated-src/antlr/main/com/risingwave/sql/parser/antlr/v4")
}

val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "11"
}
compileKotlin.dependsOn(tasks.generateGrammarSource)

val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "11"
}

tasks.spotlessJava {
    onlyIf {
        System.getenv("RISINGWAVE_FE_BUILD_ENV") == null
    }
    dependsOn(tasks.generateGrammarSource)
}

//sourceSets {
//    main {
//        withConvention(ScalaSourceSet::class) {
//            java {
//                setSrcDirs(listOf<String>())
//            }
//            scala {
//                setSrcDirs(listOf("src/main/java", "src/main/scala"))
//            }
//        }
//    }
//    test {
//        withConvention(ScalaSourceSet::class) {
//            java {
//                setSrcDirs(listOf<String>())
//            }
//            scala {
//                setSrcDirs(listOf("src/test/java", "src/test/scala"))
//            }
//        }
//    }
//}