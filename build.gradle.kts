plugins {
    id("java")
    id("application")
}

group = "org.example"
version = "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    implementation("org.json:json:20231013")
}

application {
    // Fully qualified main class for the Application plugin
    mainClass.set("org.example.Main")
}

tasks.named<JavaExec>("run") {
    // Ensure the app sees input.json at the project root when running via Gradle
    workingDir = projectDir
    // Provide default args so `gradle run` works out of the box; override with:
    //   gradle run --args="--threads 8"
    args = listOf("--threads", "4")
}

// Add Main-Class to the JAR manifest so the artifact is runnable with `java -jar` when
// dependencies are on the classpath (this is NOT a fat jar). For a single-file runnable
// JAR including dependencies, use the Shadow plugin.
tasks.jar {
    manifest {
        attributes(
            mapOf("Main-Class" to application.mainClass.get())
        )
    }
}

tasks.test {
    useJUnitPlatform()
}