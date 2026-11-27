package ai.antigem.data.agent.vibe

import android.app.Application
import ai.antigem.data.agent.AgentChatBackend
import ai.antigem.data.agent.ChatSessionStore
import ai.antigem.data.agent.opencode.a
import ai.antigem.data.agent.opencode.long
import ai.antigem.data.agent.opencode.o
import ai.antigem.data.agent.opencode.str
import ai.antigem.domain.model.AgentKind
import ai.antigem.domain.model.AiModel
import ai.antigem.domain.model.ChatMessage
import ai.antigem.domain.model.Conversation
import ai.antigem.domain.model.MessageRole
import ai.antigem.domain.model.ModelFamily
import ai.antigem.domain.model.ToolCall
import ai.antigem.domain.model.ToolType
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.io.File
import java.util.UUID

enum class VibeState(val label: String) {
    CHECKING("Checking…"),
    READY("Ready"),
    NOT_INSTALLED("Not installed"),
    DISABLED("Turned off on the server"),
    OUTDATED("Server update needed"),
    OFFLINE("Server offline"),
    ERROR("Error");

    val isBlocking get() = this == NOT_INSTALLED || this == DISABLED || this == OUTDATED || this == OFFLINE
}

data class VibePending(
    val key: String,
    val method: String,
    val params: JsonObject,
    val sessionId: String?
)

@Serializable
private data class StoredVibeChat(
    val conversation: Conversation,
    val acpSessionId: String? = null,
    val messages: List<ChatMessage> = emptyList()
)

/** Mistral Vibe ACP chat backend. */
class VibeChatBackend(
    private val application: Application,
    private val backendScope: CoroutineScope,
    private val store: ChatSessionStore,
    val prefs: VibePreferences,
    val client: VibeBridgeClient = VibeBridgeClient()
) : AgentChatBackend {

    override val kind = AgentKind.VIBE

    private val chatsDir by lazy { File(application.filesDir, "vibe_chats").apply { mkdirs() } }
    private val chats = mutableMapOf<String, StoredVibeChat>()

    private val _conversations = MutableStateFlow<List<Conversation>>(emptyList())
    override val conversations: StateFlow<List<Conversation>> = _conversations.asStateFlow()
    override val currentConversation: StateFlow<Conversation?> = store.currentConversation.asStateFlow()

    private val _messages = MutableStateFlow<List<ChatMessage>>(emptyList())
    override val messages: StateFlow<List<ChatMessage>> = _messages.asStateFlow()

    private val _isStreaming = MutableStateFlow(false)
    override val isStreaming: StateFlow<Boolean> = _isStreaming.asStateFlow()
    val isCurrentChatActivelyRunning: StateFlow<Boolean> = _isStreaming.asStateFlow()

    private val _isLoadingConversation = MutableStateFlow(false)
    override val isLoadingConversation: StateFlow<Boolean> = _isLoadingConversation.asStateFlow()

    private val _conversationError = MutableStateFlow<String?>(null)
    override val conversationError: StateFlow<String?> = _conversationError.asStateFlow()

    private val _state = MutableStateFlow(VibeState.CHECKING)
    val state: StateFlow<VibeState> = _state.asStateFlow()

    private val _status = MutableStateFlow<VibeStatus?>(null)
    val status: StateFlow<VibeStatus?> = _status.asStateFlow()

    private val _pending = MutableStateFlow<List<VibePending>>(emptyList())
    val pending: StateFlow<List<VibePending>> = _pending.asStateFlow()

    private val _availableModels = MutableStateFlow(DEFAULT_MODELS)
    val availableModels: StateFlow<List<AiModel>> = _availableModels.asStateFlow()

    private val _isRefreshingModels = MutableStateFlow(false)
    val isRefreshingModels: StateFlow<Boolean> = _isRefreshingModels.asStateFlow()

    private val _selectedModelId = MutableStateFlow(prefs.model.value)
    val selectedModelId: StateFlow<String> = _selectedModelId.asStateFlow()

    private var started = false
    private var attached = false
    private var socketJob: Job? = null
    private var activePromptJob: Job? = null
    private var lastSeq = 0L

    init {
        loadFromDisk()
    }

    fun start() {
        if (started) return
        started = true
        refreshStatus()
        startEventStream()
    }

    fun stop() {
        started = false
        socketJob?.cancel()
        socketJob = null
    }

    fun detach() {
        attached = false
    }

    fun refreshStatus() {
        backendScope.launch {
            _state.value = VibeState.CHECKING
            val res = client.status()
            res.onSuccess { st ->
                _status.value = st
                _state.value = when {
                    !st.installed -> VibeState.NOT_INSTALLED
                    else -> VibeState.READY
                }
            }.onFailure { err ->
                _state.value = when (err) {
                    is VibeUnavailableException -> when {
                        err.disabled -> VibeState.DISABLED
                        err.outdated -> VibeState.OUTDATED
                        else -> VibeState.OFFLINE
                    }
                    else -> VibeState.OFFLINE
                }
            }
        }
    }

    private fun startEventStream() {
        socketJob?.cancel()
        socketJob = backendScope.launch {
            while (isActive && started) {
                client.events(lastSeq).collect { ev ->
                    when (ev) {
                        is VibeSocketEvent.Opened -> Unit
                        is VibeSocketEvent.Frame -> handleFrame(ev.frame)
                        is VibeSocketEvent.Closed -> Unit
                    }
                }
                delay(2000)
            }
        }
    }

    private fun handleFrame(frame: JsonObject) {
        frame.long("seq")?.let { if (it > lastSeq) lastSeq = it }
        when (frame.str("type")) {
            "notification" -> {
                if (frame.str("method") == "session/update") {
                    val params = frame.o("params") ?: return
                    val sessionId = params.str("sessionId") ?: return
                    val update = params.o("update") ?: return
                    handleSessionUpdate(sessionId, update)
                }
            }
            "request" -> {
                val key = frame.str("key") ?: return
                val method = frame.str("method") ?: return
                val params = frame.o("params") ?: buildJsonObject {}
                val sessionId = frame.str("session_id")
                if (prefs.permissionMode.value == VibePreferences.MODE_AUTO && method == "session/request_permission") {
                    val optionId = pickAllowOptionId(params)
                    backendScope.launch {
                        client.reply(key, buildJsonObject {
                            put("outcome", buildJsonObject {
                                put("outcome", "selected")
                                put("optionId", optionId)
                            })
                        })
                    }
                } else {
                    _pending.value = _pending.value + VibePending(key, method, params, sessionId)
                }
            }
            "request_resolved" -> {
                val key = frame.str("key") ?: return
                _pending.value = _pending.value.filterNot { it.key == key }
            }
        }
    }

    private fun pickAllowOptionId(params: JsonObject): String {
        val options = params.a("options") ?: return "allow_once"
        for (el in options) {
            val obj = el as? JsonObject ?: continue
            val kind = obj.str("kind").orEmpty()
            val id = obj.str("optionId") ?: continue
            if (kind == "allow_once" || kind == "allow_always" || id.contains("allow", ignoreCase = true)) {
                return id
            }
        }
        return "allow_once"
    }

    private fun handleSessionUpdate(sessionId: String, update: JsonObject) {
        val chat = chats.values.find { it.acpSessionId == sessionId } ?: return
        val convId = chat.conversation.id
        val kind = update.str("sessionUpdate") ?: return
        val msgs = chat.messages.toMutableList()
        val lastIdx = msgs.indexOfLast { it.role == MessageRole.ASSISTANT && it.isStreaming }
        val current = if (lastIdx >= 0) msgs[lastIdx] else ChatMessage(
            conversationId = convId,
            role = MessageRole.ASSISTANT,
            content = "",
            isStreaming = true,
            modelId = _selectedModelId.value
        ).also { msgs.add(it) }
        val targetIdx = if (lastIdx >= 0) lastIdx else msgs.lastIndex

        val updated = when (kind) {
            "agent_message_chunk" -> {
                val text = update.o("content")?.str("text").orEmpty()
                current.copy(content = current.content + text)
            }
            "agent_thought_chunk" -> {
                val text = update.o("content")?.str("text").orEmpty()
                current.copy(thoughtText = (current.thoughtText ?: "") + text)
            }
            "tool_call", "tool_call_update" -> {
                val callId = update.str("toolCallId") ?: UUID.randomUUID().toString()
                val title = update.str("title") ?: update.str("kind") ?: "tool"
                val status = when (update.str("status")) {
                    "completed" -> "SUCCESS"
                    "failed" -> "FAILED"
                    else -> "RUNNING"
                }
                val existing = current.toolCalls.indexOfFirst { it.id == callId }
                val newCalls = current.toolCalls.toMutableList()
                val tc = ToolCall(id = callId, name = title, toolType = ToolType.BASH, command = title, status = status)
                if (existing >= 0) newCalls[existing] = newCalls[existing].copy(status = status) else newCalls.add(tc)
                current.copy(toolCalls = newCalls)
            }
            else -> current
        }
        msgs[targetIdx] = updated
        val nextChat = chat.copy(messages = msgs, conversation = chat.conversation.copy(updatedAt = System.currentTimeMillis()))
        chats[convId] = nextChat
        if (store.currentConversation.value?.id == convId && attached) {
            _messages.value = msgs
            store.messages.value = msgs
        }
    }

    override fun startNewChat() {
        attached = true
        val conv = Conversation(
            id = "vibe-" + UUID.randomUUID().toString(),
            title = "New Chat",
            modelId = _selectedModelId.value,
            agent = AgentKind.VIBE
        )
        store.currentConversation.value = conv
        _messages.value = emptyList()
        store.messages.value = emptyList()
        _conversationError.value = null
        store.conversationError.value = null
    }

    override fun selectConversation(id: String) {
        attached = true
        val stored = chats[id] ?: return
        store.currentConversation.value = stored.conversation
        _messages.value = stored.messages
        store.messages.value = stored.messages
        _conversationError.value = null
        store.conversationError.value = null
    }

    override fun sendMessage(content: String) {
        val text = content.trim()
        if (text.isEmpty()) return
        attached = true
        val cur = store.currentConversation.value?.takeIf { it.agent == AgentKind.VIBE } ?: Conversation(
            id = "vibe-" + UUID.randomUUID().toString(),
            title = text.take(40),
            modelId = _selectedModelId.value,
            agent = AgentKind.VIBE
        )
        val existing = chats[cur.id] ?: StoredVibeChat(cur)
        val title = if (existing.messages.isEmpty()) text.take(40) else existing.conversation.title
        val userMsg = ChatMessage(conversationId = cur.id, role = MessageRole.USER, content = text)
        val assistantMsg = ChatMessage(
            conversationId = cur.id,
            role = MessageRole.ASSISTANT,
            content = "",
            isStreaming = true,
            modelId = _selectedModelId.value
        )
        val nextMsgs = existing.messages + userMsg + assistantMsg
        val updatedConv = existing.conversation.copy(title = title, isRunning = true, updatedAt = System.currentTimeMillis())
        val updatedStored = existing.copy(conversation = updatedConv, messages = nextMsgs)
        chats[cur.id] = updatedStored
        publishConversations()
        store.currentConversation.value = updatedConv
        _messages.value = nextMsgs
        store.messages.value = nextMsgs
        _isStreaming.value = true
        _conversationError.value = null

        activePromptJob = backendScope.launch {
            try {
                var sessionId = chats[cur.id]?.acpSessionId
                if (sessionId == null) {
                    val newRes = client.rpc("session/new", buildJsonObject {
                        put("cwd", ".")
                        put("mcpServers", buildJsonArray {})
                    })
                    val newObj = newRes.getOrThrow().o ?: error("Invalid session/new response")
                    sessionId = newObj.str("sessionId") ?: error("Missing sessionId")
                    chats[cur.id] = chats[cur.id]!!.copy(acpSessionId = sessionId)
                    saveChatToDisk(cur.id)
                }
                val promptRes = client.rpc("session/prompt", buildJsonObject {
                    put("sessionId", sessionId)
                    put("prompt", buildJsonArray {
                        add(buildJsonObject {
                            put("type", "text")
                            put("text", text)
                        })
                    })
                }, timeoutMs = 600_000L)
                promptRes.getOrThrow()
                finishTurn(cur.id, null)
            } catch (t: Throwable) {
                finishTurn(cur.id, t.message ?: "Mistral Vibe error")
            }
        }
    }

    private fun finishTurn(convId: String, err: String?) {
        val stored = chats[convId] ?: return
        val msgs = stored.messages.map { m ->
            if (m.isStreaming) m.copy(isStreaming = false, content = if (m.content.isBlank() && err != null) "Error: $err" else m.content)
            else m
        }
        val conv = stored.conversation.copy(isRunning = false, updatedAt = System.currentTimeMillis())
        chats[convId] = stored.copy(conversation = conv, messages = msgs)
        saveChatToDisk(convId)
        publishConversations()
        if (store.currentConversation.value?.id == convId && attached) {
            store.currentConversation.value = conv
            _messages.value = msgs
            store.messages.value = msgs
            _conversationError.value = err
            store.conversationError.value = err
        }
        _isStreaming.value = false
    }

    override fun stopStreaming() {
        val curId = store.currentConversation.value?.id ?: return
        val sessionId = chats[curId]?.acpSessionId
        activePromptJob?.cancel()
        if (sessionId != null) {
            backendScope.launch {
                client.rpc("session/cancel", buildJsonObject { put("sessionId", sessionId) })
            }
        }
        finishTurn(curId, null)
    }

    override fun deleteConversation(id: String) {
        chats.remove(id)
        File(chatsDir, "$id.json").delete()
        publishConversations()
        if (store.currentConversation.value?.id == id) {
            startNewChat()
        }
    }

    override fun forkConversation(id: String) {
        val src = chats[id] ?: return
        val newId = "vibe-" + UUID.randomUUID().toString()
        val copyConv = src.conversation.copy(
            id = newId,
            title = "${src.conversation.title} (fork)",
            createdAt = System.currentTimeMillis(),
            updatedAt = System.currentTimeMillis()
        )
        chats[newId] = StoredVibeChat(copyConv, acpSessionId = null, messages = src.messages.map { it.copy(conversationId = newId) })
        saveChatToDisk(newId)
        publishConversations()
        selectConversation(newId)
    }

    fun renameConversation(id: String, title: String) {
        val stored = chats[id] ?: return
        chats[id] = stored.copy(conversation = stored.conversation.copy(title = title))
        saveChatToDisk(id)
        publishConversations()
    }

    fun setArchived(id: String, archived: Boolean) {
        val stored = chats[id] ?: return
        chats[id] = stored.copy(conversation = stored.conversation.copy(archived = archived))
        saveChatToDisk(id)
        publishConversations()
    }

    fun selectModel(modelId: String) {
        _selectedModelId.value = modelId
        prefs.setModel(modelId)
    }

    fun approvePending(item: VibePending, always: Boolean = false) {
        val opt = pickAllowOptionId(item.params)
        backendScope.launch {
            client.reply(item.key, buildJsonObject {
                put("outcome", buildJsonObject {
                    put("outcome", "selected")
                    put("optionId", if (always) "allow_always" else opt)
                })
            })
        }
    }

    fun rejectPending(item: VibePending) {
        backendScope.launch {
            client.reply(item.key, buildJsonObject {
                put("outcome", buildJsonObject { put("outcome", "cancelled") })
            })
        }
    }

    suspend fun loadMessages(conversationId: String): List<ChatMessage> = withContext(Dispatchers.IO) {
        chats[conversationId]?.messages ?: emptyList()
    }

    override fun retryConnections() {
        refreshStatus()
        startEventStream()
    }

    override fun onAppForegrounded() {
        if (started) refreshStatus()
    }

    private fun publishConversations() {
        _conversations.value = chats.values.map { it.conversation }.sortedByDescending { it.updatedAt }
    }

    private fun saveChatToDisk(id: String) {
        val chat = chats[id] ?: return
        backendScope.launch(Dispatchers.IO) {
            runCatching {
                File(chatsDir, "$id.json").writeText(VibeJson.encodeToString(chat))
            }
        }
    }

    private fun loadFromDisk() {
        backendScope.launch(Dispatchers.IO) {
            val files = chatsDir.listFiles { f -> f.extension == "json" } ?: return@launch
            for (f in files) {
                runCatching {
                    val decoded = VibeJson.decodeFromString<StoredVibeChat>(f.readText())
                    chats[decoded.conversation.id] = decoded
                }
            }
            publishConversations()
        }
    }

    companion object {
        val DEFAULT_MODELS = listOf(
            AiModel(id = "devstral-latest", displayName = "Devstral Latest", family = ModelFamily.OTHER, description = "Mistral Vibe default coding model"),
            AiModel(id = "devstral-small-latest", displayName = "Devstral Small", family = ModelFamily.OTHER, description = "Fast Mistral coding model"),
            AiModel(id = "mistral-large-latest", displayName = "Mistral Large", family = ModelFamily.OTHER, description = "Mistral flagship reasoning & coding"),
            AiModel(id = "codestral-latest", displayName = "Codestral", family = ModelFamily.OTHER, description = "Mistral code generation model")
        )
    }
}
