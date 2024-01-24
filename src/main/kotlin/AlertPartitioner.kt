import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import java.util.concurrent.ThreadLocalRandom

class AlertPartitioner: Partitioner {
    private val random = ThreadLocalRandom.current()
    override fun configure(configs: MutableMap<String, *>?) {
        return
    }

    override fun close() {
        return
    }

    override fun partition(
        topic: String?,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster?
    ): Int {

        return if ((key as Alert).alertLevel == "CRITICAL")
            findCriticalPartitionNumber()
        else
            findRandomPartition(cluster!!, topic!!)
    }

    fun findCriticalPartitionNumber(): Int{
        return 0
    }

    fun findRandomPartition(cluster: Cluster, topic: String): Int{
        val partitionList = cluster.availablePartitionsForTopic(topic)
        return random.nextInt(partitionList.size)
    }
}