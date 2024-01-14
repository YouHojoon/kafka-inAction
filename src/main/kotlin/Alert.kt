import java.io.Serializable

class Alert(
    val alertId: Int,
    val stageId: String,
    val alertLevel: String,
    val alertMessage: String
): Serializable

