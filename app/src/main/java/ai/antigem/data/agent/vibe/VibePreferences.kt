package ai.antigem.data.agent.vibe

import android.content.Context
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

/** Mistral Vibe preferences: selected model and approval mode. */
class VibePreferences(context: Context) {
    private val prefs = context.applicationContext.getSharedPreferences("vibe_prefs", Context.MODE_PRIVATE)

    private val _model = MutableStateFlow(prefs.getString(KEY_MODEL, DEFAULT_MODEL) ?: DEFAULT_MODEL)
    val model: StateFlow<String> = _model.asStateFlow()

    private val _permissionMode = MutableStateFlow(prefs.getString(KEY_MODE, MODE_AUTO) ?: MODE_AUTO)
    val permissionMode: StateFlow<String> = _permissionMode.asStateFlow()

    fun setModel(id: String) {
        prefs.edit().putString(KEY_MODEL, id).apply()
        _model.value = id
    }

    fun setPermissionMode(mode: String) {
        prefs.edit().putString(KEY_MODE, mode).apply()
        _permissionMode.value = mode
    }

    companion object {
        private const val KEY_MODEL = "model"
        private const val KEY_MODE = "permission_mode"

        const val DEFAULT_MODEL = "devstral-latest"
        const val MODE_ASK = "ask"
        const val MODE_AUTO = "auto"

        val MODES = listOf(MODE_ASK, MODE_AUTO)
    }
}
