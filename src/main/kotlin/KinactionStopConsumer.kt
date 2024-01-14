import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

class KinactionStopConsumer: Runnable {
    private val consumer: KafkaConsumer<String, String>
    private var stopping: AtomicBoolean = AtomicBoolean(false)
    init {
        val kp = Properties()
        kp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094")
        kp.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_webconsumer")
        kp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        kp.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
        kp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class)
        kp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class)
        consumer = KafkaConsumer(kp)
    }

    override fun run() {
        try {
            consumer.subscribe(listOf("kinaction_promos"))
            while (!stopping.get()){
                val records = consumer.poll(Duration.ofMillis(250))
            }
        }catch (e: WakeupException){
            if (!stopping.get())
                throw e
        }
        finally {
            consumer.close()
        }
    }

    fun shutdown(){
        stopping.set(true)
        consumer.wakeup()
    }
}