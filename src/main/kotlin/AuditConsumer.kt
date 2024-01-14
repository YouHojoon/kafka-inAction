import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

class AuditConsumer: Runnable {
    private var keepConsuming: AtomicBoolean = AtomicBoolean(true)
    private val consumer: KafkaConsumer<String, String>
    init {
        val p = Properties()
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094")
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "kinaction_auditconsumer")
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.qualifiedName)
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        consumer = KafkaConsumer<String, String>(p)
    }
    override fun run() {
        println("kinaction_audit run")
        try {
            consumer.subscribe(listOf("kinaction_audit"))
            while (keepConsuming.get()){
                val records = consumer.poll(Duration.ofMillis(250))

                for (record in records){
                    val offsetAndMetadata = OffsetAndMetadata(record.offset() + 1, "")
                    val offsetMap = HashMap<TopicPartition, OffsetAndMetadata>()
                    offsetMap.put(TopicPartition("kinaction_audit", record.partition()), offsetAndMetadata)
                    println("kinaction_audit consumer : offset = ${record.offset()} key = ${record.key()} value = ${record.value()}")
                    consumer.commitSync(offsetMap)
                }
            }
        }catch (e: WakeupException){
            if (keepConsuming.get())
                throw e
        }finally {
            consumer.close()
        }
    }
    public fun shutdown(){
        keepConsuming.getAndSet(false)
        consumer.wakeup()
    }

}