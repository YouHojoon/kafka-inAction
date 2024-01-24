import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class AlertProducer {
    private val properties: Properties = Properties()
    init {
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092, localhost:9093, localhost:9094"
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = AlertSerde::class.qualifiedName
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.qualifiedName
        properties[ProducerConfig.PARTITIONER_CLASS_CONFIG] = AlertPartitioner::class.qualifiedName
    }

    fun produce(){
        KafkaProducer<Alert, String>(properties).use {producer ->
            val alert1 = Alert(1,"Stage 1", "CRITICAL", "Stage 1 Stopped")
            val alert2 = Alert(2,"Stage 1", "WARNING", "Stage 1 Stopped")
            val alert3 = Alert(3,"Stage 1", "DEBUG", "Stage 1 Stopped")

            ProducerRecord("kinaction_alert", alert1, alert1.alertMessage).also {
                val metadata = producer.send(it).get()
                println("alert send for topic: ${metadata.topic()}, offset: ${metadata.offset()}, partition: ${metadata.partition()}")
            }

            ProducerRecord("kinaction_alert", alert2, alert2.alertMessage).also {
                val metadata = producer.send(it).get()
                println("alert send for topic: ${metadata.topic()}, offset: ${metadata.offset()}, partition: ${metadata.partition()}")
            }

            ProducerRecord("kinaction_alert", alert3, alert3.alertMessage).also {
                val metadata = producer.send(it).get()
                println("alert send for topic: ${metadata.topic()}, offset: ${metadata.offset()}, partition: ${metadata.partition()}")
            }
        }
    }
}