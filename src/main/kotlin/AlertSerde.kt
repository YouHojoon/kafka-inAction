import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.StandardCharsets

class AlertSerde:
    Serializer<Alert>, Deserializer<Alert> {
    override fun serialize(topic: String?, data: Alert?): ByteArray {
        if (data == null)
            return ByteArray(0)
        return data.stageId.toByteArray(StandardCharsets.UTF_8)
    }

    override fun close() {
        return
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        return
    }

    override fun deserialize(topic: String?, data: ByteArray?): Alert? {
        return null
    }
}