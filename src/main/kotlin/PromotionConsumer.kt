import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import kotlin.concurrent.Volatile

class PromotionConsumer{
    @Volatile
    private var keepConsuming: Boolean = false
    private val kp: Properties = Properties()

    companion object{
        fun commitOffset(offset: Long, partition: Int, topic: String, consumer: KafkaConsumer<String, String>){
            val offsetMeta = OffsetAndMetadata(offset + 1, "")
            val kaOffsetMap = HashMap<TopicPartition, OffsetAndMetadata>()
            kaOffsetMap.put(TopicPartition(topic, partition), offsetMeta)

            consumer.commitAsync(kaOffsetMap){map, e ->
                if (e != null){
                    for (key in map.keys){
                        println("kinaction_error offset = ${map.get(key)!!.offset()}")
                    }
                }
                else{
                    for (key in map.keys){
                        println("kinaction_info offset = ${map.get(key)!!.offset()}")
                    }
                }
            }
        }
    }

    init {
        kp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9093, localhost:9094")
        kp.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_webconsumer")
        kp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        kp.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
        kp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class)
        kp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class)
    }

    fun consume(){
        val consumer = KafkaConsumer<String,String>(kp)
        consumer.subscribe(listOf("kinaction_promos"))

        while (keepConsuming){
            val records = consumer.poll(Duration.ofMillis(250))

            for (record in records){
                println("kinaction_info offset = ${record.offset()}, key = ${record.key()}")
                println("kinaction_info value = ${record.value()}")
            }
        }

        Runtime.getRuntime().addShutdownHook(Thread(this::shutdown))
    }

    private fun shutdown(){
        keepConsuming = false
    }

}