package zed.rainxch.core.domain.system

interface SystemInstallSerializer {
    suspend fun awaitFreeOrTimeout(timeoutMs: Long = DEFAULT_TIMEOUT_MS)

    fun markPending(packageName: String)

    fun markCompleted(packageName: String)

    companion object {
        const val DEFAULT_TIMEOUT_MS: Long = 60_000L
    }
}
