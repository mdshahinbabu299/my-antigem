package ai.antigem.ui.vibe

import android.widget.Toast
import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.FilterChip
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import ai.antigem.data.agent.vibe.VibeChatBackend
import ai.antigem.data.agent.vibe.VibeCliJob
import ai.antigem.data.agent.vibe.VibePreferences
import ai.antigem.data.agent.vibe.VibeState
import ai.antigem.ui.components.VibeAccent
import ai.antigem.ui.components.bouncyVerticalScroll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

@Composable
fun VibeSettingsScreen(backend: VibeChatBackend) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    val state by backend.state.collectAsState()
    val status by backend.status.collectAsState()
    val mode by backend.prefs.permissionMode.collectAsState()

    var job by remember { mutableStateOf<VibeCliJob?>(null) }
    var apiKey by remember { mutableStateOf("") }
    var savingKey by remember { mutableStateOf(false) }

    LaunchedEffect(Unit) {
        backend.refreshStatus()
        backend.client.cliJob().onSuccess { job = it }
    }

    LaunchedEffect(job?.running) {
        if (job?.running == true) {
            while (true) {
                delay(1200)
                val next = backend.client.cliJob().getOrNull() ?: break
                job = next
                if (!next.running) {
                    backend.refreshStatus()
                    break
                }
            }
        }
    }

    Column(
        Modifier
            .fillMaxSize()
            .bouncyVerticalScroll(rememberScrollState())
            .padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        Surface(
            shape = RoundedCornerShape(16.dp),
            color = MaterialTheme.colorScheme.surfaceContainerLow,
            border = BorderStroke(1.dp, MaterialTheme.colorScheme.outline.copy(alpha = 0.14f)),
            modifier = Modifier.fillMaxWidth()
        ) {
            Column(Modifier.padding(14.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text("Mistral Vibe CLI", fontSize = 15.sp, fontWeight = FontWeight.SemiBold)
                Text(
                    text = when {
                        state == VibeState.READY -> "Installed (${status?.version ?: "vibe-acp"})"
                        state == VibeState.NOT_INSTALLED -> "Not installed yet. Install mistral-vibe below (requires uv, pipx, or python3-pip)."
                        else -> state.label
                    },
                    fontSize = 12.5.sp,
                    color = if (state == VibeState.READY) Color(0xFF10B981) else MaterialTheme.colorScheme.onSurfaceVariant
                )
                status?.binary?.let {
                    Text("Binary: $it", fontSize = 11.sp, fontFamily = FontFamily.Monospace, color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    val installing = job?.running == true
                    Button(
                        enabled = !installing,
                        colors = ButtonDefaults.buttonColors(containerColor = VibeAccent),
                        onClick = {
                            scope.launch {
                                backend.client.startCliInstall(update = status?.installed == true)
                                    .onSuccess { job = it }
                                    .onFailure { Toast.makeText(context, it.message ?: "Install failed", Toast.LENGTH_LONG).show() }
                            }
                        }
                    ) {
                        if (installing) {
                            CircularProgressIndicator(Modifier.size(16.dp), strokeWidth = 2.dp, color = Color.White)
                            Spacer(Modifier.width(8.dp))
                        }
                        Text(if (status?.installed == true) "Update CLI" else "Install mistral-vibe")
                    }
                    OutlinedButton(onClick = { backend.refreshStatus() }) {
                        Text("Refresh")
                    }
                }
                job?.log?.takeIf { it.isNotBlank() }?.let { logText ->
                    Surface(
                        shape = RoundedCornerShape(8.dp),
                        color = MaterialTheme.colorScheme.surfaceContainerHighest,
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Text(
                            text = logText.takeLast(1500),
                            fontSize = 10.5.sp,
                            fontFamily = FontFamily.Monospace,
                            modifier = Modifier.padding(8.dp)
                        )
                    }
                }
            }
        }

        Surface(
            shape = RoundedCornerShape(16.dp),
            color = MaterialTheme.colorScheme.surfaceContainerLow,
            border = BorderStroke(1.dp, MaterialTheme.colorScheme.outline.copy(alpha = 0.14f)),
            modifier = Modifier.fillMaxWidth()
        ) {
            Column(Modifier.padding(14.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text("Authentication (Mistral Pro / API Key)", fontSize = 15.sp, fontWeight = FontWeight.SemiBold)
                Text(
                    "For your Mistral Pro subscription (All-day coding in CLI): open the Terminal tab and run `vibe --setup` once to sign in via browser. " +
                        "Or paste a Mistral API key below:",
                    fontSize = 12.sp,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                OutlinedTextField(
                    value = apiKey,
                    onValueChange = { apiKey = it },
                    label = { Text("MISTRAL_API_KEY (optional)") },
                    visualTransformation = PasswordVisualTransformation(),
                    singleLine = true,
                    modifier = Modifier.fillMaxWidth()
                )
                Button(
                    enabled = apiKey.isNotBlank() && !savingKey,
                    onClick = {
                        savingKey = true
                        scope.launch {
                            backend.client.setApiKey(apiKey)
                                .onSuccess {
                                    apiKey = ""
                                    backend.refreshStatus()
                                    Toast.makeText(context, "Saved to ~/.vibe/.env", Toast.LENGTH_SHORT).show()
                                }
                                .onFailure { Toast.makeText(context, it.message ?: "Failed", Toast.LENGTH_LONG).show() }
                            savingKey = false
                        }
                    }
                ) { Text("Save API Key") }
            }
        }

        Surface(
            shape = RoundedCornerShape(16.dp),
            color = MaterialTheme.colorScheme.surfaceContainerLow,
            border = BorderStroke(1.dp, MaterialTheme.colorScheme.outline.copy(alpha = 0.14f)),
            modifier = Modifier.fillMaxWidth()
        ) {
            Column(Modifier.padding(14.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text("Tool Permissions", fontSize = 15.sp, fontWeight = FontWeight.SemiBold)
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    VibePreferences.MODES.forEach { m ->
                        FilterChip(
                            selected = mode == m,
                            onClick = { backend.prefs.setPermissionMode(m) },
                            label = { Text(if (m == VibePreferences.MODE_AUTO) "Auto-approve" else "Ask before tools") }
                        )
                    }
                }
            }
        }
        Spacer(Modifier.height(16.dp))
    }
}
