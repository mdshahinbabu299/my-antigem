package ai.antigem.ui.vibe

import androidx.compose.foundation.BorderStroke
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import ai.antigem.data.agent.opencode.o
import ai.antigem.data.agent.opencode.str
import ai.antigem.data.agent.vibe.VibeChatBackend
import ai.antigem.data.agent.vibe.VibePending
import ai.antigem.data.agent.vibe.VibePreferences
import ai.antigem.ui.components.VibeAccent

@Composable
fun VibeControlsStrip(backend: VibeChatBackend) {
    val mode by backend.prefs.permissionMode.collectAsState()
    val state by backend.state.collectAsState()
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 12.dp, vertical = 4.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Surface(
            onClick = {
                val next = if (mode == VibePreferences.MODE_AUTO) VibePreferences.MODE_ASK else VibePreferences.MODE_AUTO
                backend.prefs.setPermissionMode(next)
            },
            shape = RoundedCornerShape(12.dp),
            color = VibeAccent.copy(alpha = 0.12f),
            border = BorderStroke(1.dp, VibeAccent.copy(alpha = 0.35f))
        ) {
            Text(
                text = if (mode == VibePreferences.MODE_AUTO) "Vibe · Auto" else "Vibe · Ask",
                fontSize = 11.5.sp,
                fontWeight = FontWeight.Medium,
                color = VibeAccent,
                modifier = Modifier.padding(horizontal = 10.dp, vertical = 4.dp)
            )
        }
        Text(
            text = state.label,
            fontSize = 11.sp,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
    }
}

@Composable
fun VibeApprovalCard(
    pending: VibePending,
    onApprove: () -> Unit,
    onAlwaysAllow: () -> Unit,
    onReject: () -> Unit
) {
    val title = pending.params.o("toolCall")?.str("title") ?: pending.method
    Surface(
        shape = RoundedCornerShape(14.dp),
        color = MaterialTheme.colorScheme.surfaceContainerHigh,
        border = BorderStroke(1.dp, VibeAccent.copy(alpha = 0.5f)),
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 12.dp, vertical = 6.dp)
    ) {
        Column(Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Mistral Vibe permission request", fontSize = 13.sp, fontWeight = FontWeight.SemiBold, color = VibeAccent)
            Text(title, fontSize = 12.5.sp)
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(
                    onClick = onApprove,
                    colors = ButtonDefaults.buttonColors(containerColor = VibeAccent)
                ) { Text("Allow") }
                OutlinedButton(onClick = onAlwaysAllow) { Text("Always") }
                Spacer(Modifier.width(4.dp))
                OutlinedButton(onClick = onReject) { Text("Deny") }
            }
        }
    }
}
