package ai.antigem.data.agent.vibe

import ai.antigem.data.agent.opencode.bool
import ai.antigem.data.agent.opencode.long
import ai.antigem.data.agent.opencode.o
import ai.antigem.data.agent.opencode.str
import ai.antigem.data.preferences.AuthPreferences
import ai.antigem.data.remote.AgyBridgeService
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import java.util.concurrent.TimeUnit

val VibeJson = Json { ignoreUnknownKeys = true; explicitNulls = false; encodeDefaults = true; isLenient = true }

class VibeUnavailableException(message: String, val disabled: Boolean = false, val outdated: Boolean = false) : Exception(message)

class VibeRpcException(message: String, val code: Int? = null, val unavailable: Boolean = false) : Exception(message)

@Serializable
data class VibeStatus(
    val success: Boolean = false,
    val installed: Boolean = false,
    val running: Boolean = false,
    val starting: Boolean = false,
    val binary: String? = null,
    val version: String? = null,
    val error: String? = null,
    val logs: List<String>? = null,
    val uptime_seconds: Long? = null,
    val auth_configured: Boolean = false,
    val termux_package: Boolean = false
)

@Serializable
data class VibeCliJob(
    val kind: String = "",
    val running: Boolean = false,
    val exit_code: Int? = null,
    val log: String = ""
)

@Serializable
private data class VibeCliJobResponse(val success: Boolean = false, val error: String? = null, val job: VibeCliJob? = null)

@Serializable
private data class VibeSimpleResponse(val success: Boolean = false, val error: String? = null)

sealed class VibeSocketEvent {
    data object Opened : VibeSocketEvent()
    data class Frame(val frame: JsonObject) : VibeSocketEvent()
    data class Closed(val error: String?, val disabled: Boolean = false, val outdated: Boolean = false) : VibeSocketEvent()
}

/** Talks to the bridge's `/api/vibe/` endpoints (ACP over JSON-RPC 2.0). */
class VibeBridgeClient(private val bridge: AgyBridgeService = AgyBridgeService.instance) {
    private val jsonType = "application/json".toMediaType()

    private fun url(path: String) = AuthPreferences.currentBridgeHttpUrl.trimEnd('/') + "/api/vibe" + path

    private val client by lazy { bridge.client.newBuilder().readTimeout(150, TimeUnit.SECONDS).build() }

    private suspend fun execute(request: Request): String = withContext(Dispatchers.IO) {
        client.newCall(request).execute().use { resp ->
            val text = resp.body?.string().orEmpty()
            val isJson = text.trimStart().let { it.startsWith("{") || it.startsWith("[") }
            if (resp.code == 404 && !isJson) {
                throw VibeUnavailableException("This server has no Mistral Vibe support yet. Update and restart the server.", outdated = true)
            }
            if (resp.code == 503 && text.contains("\"disabled\":true")) {
                throw VibeUnavailableException("The server was started with Mistral Vibe turned off. Restart it to use it.", disabled = true)
            }
            if (!resp.isSuccessful && !isJson) error(text.trim().take(200).ifBlank { "HTTP ${resp.code}" })
            text
        }
    }

    private fun post(path: String, body: JsonElement) =
        Request.Builder().url(url(path)).post(VibeJson.encodeToString(JsonElement.serializer(), body).toRequestBody(jsonType)).build()

    suspend fun status(): Result<VibeStatus> = runCatching {
        val text = execute(Request.Builder().url(url("/status")).get().build())
        VibeJson.decodeFromString(VibeStatus.serializer(), text)
    }

    suspend fun start(): Result<Unit> = runCatching {
        val text = execute(post("/start", buildJsonObject {}))
        val parsed = VibeJson.decodeFromString(VibeSimpleResponse.serializer(), text)
        if (!parsed.success) error(parsed.error ?: "Failed to start vibe-acp")
    }

    suspend fun restart(): Result<Unit> = runCatching {
        val text = execute(post("/restart", buildJsonObject {}))
        val parsed = VibeJson.decodeFromString(VibeSimpleResponse.serializer(), text)
        if (!parsed.success) error(parsed.error ?: "Failed to restart vibe-acp")
    }

    suspend fun setApiKey(apiKey: String): Result<Unit> = runCatching {
        val text = execute(post("/auth/apikey", buildJsonObject { put("api_key", apiKey) }))
        val parsed = VibeJson.decodeFromString(VibeSimpleResponse.serializer(), text)
        if (!parsed.success) error(parsed.error ?: "Failed to save Mistral API key")
    }

    suspend fun startCliInstall(update: Boolean): Result<VibeCliJob> = runCatching {
        val path = if (update) "/cli/update" else "/cli/install"
        val text = execute(post(path, buildJsonObject {}))
        val parsed = VibeJson.decodeFromString(VibeCliJobResponse.serializer(), text)
        if (!parsed.success || parsed.job == null) error(parsed.error ?: "Failed to start Mistral Vibe install")
        parsed.job
    }

    suspend fun cliJob(): Result<VibeCliJob?> = runCatching {
        val text = execute(Request.Builder().url(url("/cli/job")).get().build())
        val parsed = VibeJson.decodeFromString(VibeCliJobResponse.serializer(), text)
        if (!parsed.success) error(parsed.error ?: "Failed to read install job")
        parsed.job
    }

    suspend fun rpc(method: String, params: JsonElement? = null, timeoutMs: Long? = null): Result<JsonElement> = runCatching {
        val body = buildJsonObject {
            put("method", method)
            if (params != null) put("params", params)
            if (timeoutMs != null) put("timeout_ms", timeoutMs)
        }
        val text = execute(post("/rpc", body))
        val obj = VibeJson.parseToJsonElement(text).o ?: error("Invalid bridge response")
        if (obj.bool("success") != true) {
            throw VibeRpcException(
                message = obj.str("error") ?: "Mistral Vibe RPC failed",
                code = obj.long("code")?.toInt(),
                unavailable = obj.bool("vibe_unavailable") == true
            )
        }
        obj["result"] ?: JsonNull
    }

    suspend fun reply(key: String, result: JsonElement? = null, error: JsonObject? = null): Result<Unit> = runCatching {
        val body = buildJsonObject {
            if (result != null) put("result", result)
            if (error != null) put("error", error)
        }
        val text = execute(post("/requests/$key/reply", body))
        val parsed = VibeJson.decodeFromString(VibeSimpleResponse.serializer(), text)
        if (!parsed.success) error(parsed.error ?: "Failed to send reply")
    }

    fun events(since: Long = 0): Flow<VibeSocketEvent> = callbackFlow {
        val wsUrl = url("/ws?since=$since").replaceFirst("http://", "ws://").replaceFirst("https://", "wss://")
        val req = Request.Builder().url(wsUrl).build()
        val ws = client.newWebSocket(req, object : WebSocketListener() {
            override fun onOpen(webSocket: WebSocket, response: Response) {
                trySend(VibeSocketEvent.Opened)
            }

            override fun onMessage(webSocket: WebSocket, text: String) {
                val frame = runCatching { VibeJson.parseToJsonElement(text).o }.getOrNull() ?: return
                trySend(VibeSocketEvent.Frame(frame))
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                val code = response?.code
                val msg = t.message ?: response?.message ?: "WebSocket closed"
                trySend(VibeSocketEvent.Closed(msg, disabled = code == 503, outdated = code == 404))
                close()
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                trySend(VibeSocketEvent.Closed(reason.ifBlank { null }))
                close()
            }
        })
        awaitClose { ws.close(1000, null) }
    }.buffer(Channel.UNLIMITED)
}
