import org.junit.ClassRule
import org.apache.kafka.streams.in

class EmbeddedKafkaClusterTest {
    @ClassRule
    val embeddedKafkaCluster: Embedded  = EmbeddedKafkaClusterTest()
}