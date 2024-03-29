import com.github.davidmc24.gradle.plugin.avro.GenerateAvroJavaTask
import java.net.URI

plugins {
    kotlin("jvm") version "1.9.0"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven{
        url = URI("https://packages.confluent.io/maven")
    }
}

dependencies {
    // https://mvnrepository.com/artifact/org.apache.avro/avro
    implementation("org.apache.avro:avro:1.11.3")
    implementation("io.confluent:kafka-avro-serializer:7.5.1")
    implementation("org.apache.kafka:kafka-streams:3.6.1")
    implementation("org.apache.commons:commons-lang3:3.14.0")

    testImplementation(kotlin("test"))

    testImplementation("junit:junit:4.13.2")
    compileOnly("org.apache.kafka:kafka-clients:3.6.1")
    testCompileOnly("org.apache.kafka:kafka-clients:3.6.1")
    testCompileOnly("org.apache.kafka:kafka-streams:3.6.1")
    compileOnly("org.apache.kafka:kafka-streams-test-utils:3.6.1")
    testCompileOnly("org.apache.kafka:kafka_2.12:3.6.1")

}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(8)
}

avro {
    isCreateSetters.set(true)
    isCreateOptionalGetters.set(false)
    isGettersReturnOptional.set(false)
    isOptionalGettersForNullableFieldsOnly.set(false)
    fieldVisibility.set("PUBLIC")
    outputCharacterEncoding.set("UTF-8")
    stringType.set("String")
    templateDirectory.set(null as String?)
    isEnableDecimalLogicalType.set(true)
}

tasks.withType(JavaCompile::class).configureEach {
    options.encoding = "UTF-8"
}

//tasks {
//    register("generateAvro", GenerateAvroJavaTask::class){
//        source("src/avro")
//        setOutputDir(file("src/main/avro"))
//    }
//}
