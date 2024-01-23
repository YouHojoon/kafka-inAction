import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

class AlertConsumer: Runnable {
    private val consumer: KafkaConsumer<Alert, String>
    private val keepConsume: AtomicBoolean = AtomicBoolean(true)
    init {
        val p = Properties()
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094")
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_alertconsumer")
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AlertSerde::class.qualifiedName)
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
        consumer = KafkaConsumer(p)
    }

    override fun run() {
        while (keepConsume.get()){
            val records = consumer.poll(Duration.ofMillis(250))

            for (record in records){

            }
        }
    }


}