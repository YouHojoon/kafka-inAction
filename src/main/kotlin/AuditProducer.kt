import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.*

class AuditProducer {

    private val properties = Properties()
    init {
        properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
        properties.put("key.serializer",StringSerializer::class.qualifiedName)
        properties.put("value.serializer",StringSerializer::class.qualifiedName)
//        properties.put("schema.registry.url","http://localhost:8081")
        properties.put("acks","all")
        properties.put("retries","3")
        properties.put("max.in.flight.requests.per.connection", "1")
    }

    fun produce(){
        KafkaProducer<String, String>(properties).use {producer ->
            val alert = Alert(0, "Stage 0", "CRITICAL", "Stage 0 stopped")
            println("kinaction_info Alert")
            ProducerRecord("kinaction_audit", alert.stageId, alert.alertMessage).also {
                val metadata = producer.send(it).get()
                println("kinaction info offset ${metadata.offset()} topic ${metadata.topic()} timestamp ${metadata.timestamp()}")
            }
        }
    }
}