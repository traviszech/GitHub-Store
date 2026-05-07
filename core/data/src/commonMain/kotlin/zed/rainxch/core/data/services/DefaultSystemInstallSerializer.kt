package zed.rainxch.core.data.services

import co.touchlab.kermit.Logger
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeoutOrNull
import zed.rainxch.core.domain.system.SystemInstallSerializer

class DefaultSystemInstallSerializer : SystemInstallSerializer {
    private val pending = MutableStateFlow<String?>(null)

    override suspend fun awaitFreeOrTimeout(timeoutMs: Long) {
        if (pending.value == null) return
        val freed =
            withTimeoutOrNull(timeoutMs) {
                pending.first { it == null }
            }
        if (freed == null) {
            Logger.w {
                "SystemInstallSerializer: timed out waiting for ${pending.value} to clear; force-releasing"
            }
            pending.value = null
        }
    }

    override fun markPending(packageName: String) {
        pending.value = packageName
    }

    override fun markCompleted(packageName: String) {
        pending.compareAndSet(packageName, null)
    }
}
