import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.Duration
import java.time.Instant
import java.util.Properties

class HelloWorldConsumer(
    @Volatile
    private var keepConsuming: Boolean = true
) {
    private val properties: Properties = Properties()
    init {
        properties.put("bootstrap.servers", arrayOf("localhost:9094"))
        properties.put("key.deserializer","org.apache.kafka.common.serialization.LongDeserializer")
        properties.put("value.deserializer","io.confluent.kafka.KafkaAvroDeserializer")
        properties.put("schema.registry.url","http://localhost:8081")
    }
    public fun consume(){
        KafkaConsumer<Long, Alert>(properties).use {
            it.subscribe(listOf("kinaction_schematest"))

            while (keepConsuming){
                val records = it.poll(Duration.ofMillis(250))

                for (record in records){
                    println("kinaction_info offset = ${record.offset()}, value = ${record.value()}")
                }
            }
        }

        Runtime.getRuntime().addShutdownHook(Thread(this::shutdown))
    }

    private fun shutdown(){
        keepConsuming = false
    }
}

fun main(){
    val properties = Properties()
    properties.put("bootstrap.servers", arrayOf("localhost:9092","localhost:9093","localhost:9094"))
    properties.put("key.serializer","org.apache.kafka.common.serialization.LongSerializer")
    properties.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer")
    properties.put("schema.registry.url","http://localhost:8081")

    KafkaProducer<Long,Alert>(properties).use {producer ->
        val alert = Alert(12345L, Instant.now().toEpochMilli(), AlertStatus.CRITICAL)
        println("kinaction_info Alert")
        ProducerRecord("kinaction_schematest", alert.sensorId, alert).also {
            producer.send(it)
        }
    }

    println(properties.toString())
}