import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

class AlertTrendingProducer {
    private val kp = Properties()

    init {
        kp.put("bootstrap.servers","localhost:9092, localhost:9093, localhost:9094")
        kp.put("key.serializer", AlertSerde::class.qualifiedName)
        kp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    }

    fun produce(){
        KafkaProducer<Alert,String>(kp).use {producer ->
            val alert = Alert(0, "Stage 0", "CRITICAL", "Stage 0 stopped")
            val record = ProducerRecord<Alert, String>("kinaction_alerttrend", alert, alert.alertMessage)
            val result = producer.send(record).get()

            println("kinaction_info offset = ${result.offset()}, topic = ${result.topic()}, timestamp = ${result.timestamp()}")
        }
    }
}