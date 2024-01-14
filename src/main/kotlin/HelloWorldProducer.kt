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
        properties.put("bootstrap.servers", "localhost:9094")
        properties.put("group.id","kinaction_helloconsumer")
        properties.put("enable.auto.commit","true")
        properties.put("auto.commit.interval.ms", "1000")
        properties.put("key.deserializer","org.apache.kafka.common.serialization.LongDeserializer")
        properties.put("value.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer")
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
    val consumer = AuditConsumer()
    val producer = AuditProducer()

    val thread = Thread(consumer)
    thread.start()
    thread.join()

    producer.produce()
}