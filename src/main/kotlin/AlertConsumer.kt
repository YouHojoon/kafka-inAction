import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

class AlertConsumer: Runnable {
    private val consumer: KafkaConsumer<Alert, String>
    private val keepConsume: AtomicBoolean = AtomicBoolean(true)
    init {
        val p = Properties()
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094")
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_alertconsumer")
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AlertSerde::class.qualifiedName)
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
        consumer = KafkaConsumer(p)
        consumer.assign(listOf(TopicPartition("kinaction_alert",0)))
    }

    override fun run() {
        try {
            while (keepConsume.get()){
                val records = consumer.poll(Duration.ofMillis(250))

                for (record in records){
                    println("partition: ${record.partition()}, offset: ${record.offset()}, value: ${record.value()}")
                    commitOffset(record.offset(), record.partition(), record.topic(), consumer)
                }
            }
        }catch (e: WakeupException){
            if (keepConsume.get())
                throw e
        }
        finally {
            consumer.close()
        }
    }

    fun commitOffset(offset: Long, partition: Int, topic: String, consumer: KafkaConsumer<Alert, String>){
        val offsetMetadata = OffsetAndMetadata(offset + 1, "")
        val offsetMap = hashMapOf(TopicPartition(topic, partition) to offsetMetadata)

        consumer.commitAsync(offsetMap){m,e ->

        }
    }
    fun shutdown(){
        keepConsume.set(false)
        consumer.wakeup()
    }
}