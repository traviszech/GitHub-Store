package zed.rainxch.core.data.services.root

import android.os.ParcelFileDescriptor
import co.touchlab.kermit.Logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import zed.rainxch.core.data.services.root.model.RootStatus
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.util.concurrent.TimeUnit

class RootServiceManager(
    private val scope: CoroutineScope,
) {
    private val _status = MutableStateFlow(RootStatus.NOT_AVAILABLE)
    val status: StateFlow<RootStatus> = _status.asStateFlow()

    @Volatile
    private var cachedSuPath: String? = null

    fun initialize() {
        // First-launch detection runs off the main thread because `su -c id`
        // can block for several seconds when the root manager (Magisk /
        // KernelSU / APatch) is showing its grant dialog or doing initial
        // bookkeeping. We don't want to stall app cold-start.
        scope.launch(Dispatchers.IO) {
            refreshStatusBlocking()
        }
    }

    fun refreshStatus() {
        scope.launch(Dispatchers.IO) { refreshStatusBlocking() }
    }

    /**
     * Triggers the root manager's per-app authorization dialog without
     * actually performing a privileged action. Idiomatic across Magisk /
     * KernelSU / APatch — running `su -c true` is enough to make the
     * manager pop its allow/deny prompt; the grant is then cached and
     * subsequent calls run silently.
     *
     * No-op when the su binary isn't on the device. Re-runs status
     * detection afterwards so the UI reflects the user's choice.
     */
    fun requestPermission() {
        scope.launch(Dispatchers.IO) {
            val su = cachedSuPath ?: locateSuBinary() ?: run {
                Logger.d(TAG) { "requestPermission() — no su binary on device, skipping" }
                refreshStatusBlocking()
                return@launch
            }
            try {
                Logger.d(TAG) { "requestPermission() — invoking '$su -c true' to surface root prompt" }
                val proc = Runtime.getRuntime().exec(arrayOf(su, "-c", "true"))
                proc.waitFor(PROMPT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                if (proc.isAlive) {
                    Logger.w(TAG) { "requestPermission() — prompt invocation still running after ${PROMPT_TIMEOUT_SECONDS}s, destroying" }
                    proc.destroyForcibly()
                }
            } catch (e: Exception) {
                Logger.w(TAG) { "requestPermission() failed: ${e.javaClass.simpleName}: ${e.message}" }
            }
            refreshStatusBlocking()
        }
    }

    /**
     * Pipes [apkFile] into `pm install -S <size> -i <pkg> -` over `su`.
     *
     * Stdin streaming is deliberate: invoking `pm install <path>` against
     * an app-private APK location fails because the shell process — even
     * running as UID 0 — is denied by SELinux on `app_data_files`. Reading
     * the bytes from app process and writing them to `pm`'s stdin sidesteps
     * the policy.
     *
     * Returns `0` on success, non-zero on failure, `null` when no su binary
     * is reachable. Mirrors the AIDL contract of the Shizuku / Dhizuku
     * services so [SilentInstallerDispatcher] can treat all silent backends
     * uniformly.
     */
    suspend fun installPackage(
        apkFile: File,
        installerPackageName: String?,
    ): Int? =
        withContext(Dispatchers.IO) {
            val su = cachedSuPath ?: locateSuBinary() ?: run {
                Logger.w(TAG) { "installPackage() — no su binary available" }
                return@withContext null
            }
            val safeInstaller = installerPackageName?.takeIf { it.isNotBlank() }
            val pm = "/system/bin/pm"
            // Always shell out the full pm path — some Magisk modules /
            // KernelSU configurations strip `/system/bin` from the minimal
            // PATH that `su -c <cmd>` runs against, and the resulting
            // `pm: not found` (exit 127) is a silent class of bug.
            val command = buildString {
                append(pm).append(" install ")
                if (safeInstaller != null) append("-i ").append(safeInstaller).append(' ')
                append("-S ").append(apkFile.length()).append(' ')
                append('-')
            }
            Logger.d(TAG) { "installPackage() — executing via $su: $command" }
            val proc = try {
                Runtime.getRuntime().exec(arrayOf(su, "-c", command))
            } catch (e: Exception) {
                Logger.e(TAG) { "installPackage() — su exec failed: ${e.message}" }
                return@withContext null
            }

            val pipeError = StringBuilder()
            val pipeThread = Thread {
                try {
                    apkFile.inputStream().use { input ->
                        proc.outputStream.use { output ->
                            input.copyTo(output)
                        }
                    }
                } catch (e: Exception) {
                    pipeError.append(e.javaClass.simpleName).append(": ").append(e.message)
                    Logger.e(TAG) { "installPackage() — stdin pipe failed: $pipeError" }
                }
            }
            pipeThread.start()
            pipeThread.join(INSTALL_TIMEOUT_SECONDS * 1000L)
            if (pipeThread.isAlive) {
                Logger.e(TAG) { "installPackage() — pipe thread still alive after ${INSTALL_TIMEOUT_SECONDS}s, destroying process" }
                pipeThread.interrupt()
                proc.destroyForcibly()
                return@withContext STATUS_FAILURE
            }

            val stdout = BufferedReader(InputStreamReader(proc.inputStream)).readText().trim()
            val stderr = BufferedReader(InputStreamReader(proc.errorStream)).readText().trim()
            val finished = proc.waitFor(INSTALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            if (!finished) {
                Logger.e(TAG) { "installPackage() — pm process timed out, destroying" }
                proc.destroyForcibly()
                return@withContext STATUS_FAILURE
            }
            val exit = proc.exitValue()
            Logger.d(TAG) { "installPackage() — exit=$exit stdout='$stdout' stderr='$stderr'" }
            if (exit == 0 && stdout.contains("Success")) {
                STATUS_SUCCESS
            } else {
                Logger.w(TAG) { "installPackage() — pm reported failure: stdout='$stdout' stderr='$stderr'" }
                STATUS_FAILURE
            }
        }

    suspend fun uninstallPackage(packageName: String): Int? =
        withContext(Dispatchers.IO) {
            val su = cachedSuPath ?: locateSuBinary() ?: return@withContext null
            val command = "/system/bin/pm uninstall $packageName"
            try {
                val proc = Runtime.getRuntime().exec(arrayOf(su, "-c", command))
                val stdout = BufferedReader(InputStreamReader(proc.inputStream)).readText().trim()
                val finished = proc.waitFor(UNINSTALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                if (!finished) {
                    proc.destroyForcibly()
                    return@withContext STATUS_FAILURE
                }
                val exit = proc.exitValue()
                Logger.d(TAG) { "uninstallPackage($packageName) — exit=$exit stdout='$stdout'" }
                if (exit == 0 && stdout.contains("Success")) STATUS_SUCCESS else STATUS_FAILURE
            } catch (e: Exception) {
                Logger.e(TAG) { "uninstallPackage($packageName) — su exec failed: ${e.message}" }
                STATUS_FAILURE
            }
        }

    private fun refreshStatusBlocking() {
        val computed = computeStatus()
        if (_status.value != computed) {
            Logger.d(TAG) { "refreshStatus() — $computed (was ${_status.value})" }
        }
        _status.value = computed
    }

    private fun computeStatus(): RootStatus {
        val su = locateSuBinary() ?: run {
            cachedSuPath = null
            return RootStatus.NOT_AVAILABLE
        }
        cachedSuPath = su
        // `su -c id` is the standard probe. We wrap it in a short timeout
        // because a denied request on some root managers can hang waiting
        // for the user to dismiss the dialog. NOT_RUNNING isn't a state we
        // model for root (unlike Shizuku); a denial is reported as
        // PERMISSION_NEEDED so the UI prompts the user to grant.
        return try {
            val proc = Runtime.getRuntime().exec(arrayOf(su, "-c", "id"))
            val finished = proc.waitFor(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            if (!finished) {
                Logger.d(TAG) { "computeStatus() — su probe timed out, treating as PERMISSION_NEEDED" }
                proc.destroyForcibly()
                return RootStatus.PERMISSION_NEEDED
            }
            val output = BufferedReader(InputStreamReader(proc.inputStream)).readText().trim()
            val exit = proc.exitValue()
            when {
                exit == 0 && output.contains("uid=0") -> RootStatus.READY
                else -> RootStatus.PERMISSION_NEEDED
            }
        } catch (e: Exception) {
            Logger.w(TAG) { "computeStatus() — su probe threw: ${e.javaClass.simpleName}: ${e.message}" }
            RootStatus.PERMISSION_NEEDED
        }
    }

    private fun locateSuBinary(): String? {
        for (path in SU_PATHS) {
            if (File(path).exists()) return path
        }
        return null
    }

    companion object {
        private const val TAG = "RootServiceManager"
        private const val STATUS_SUCCESS = 0
        private const val STATUS_FAILURE = -1
        private const val INSTALL_TIMEOUT_SECONDS = 120L
        private const val UNINSTALL_TIMEOUT_SECONDS = 30L
        private const val PROBE_TIMEOUT_SECONDS = 5L
        private const val PROMPT_TIMEOUT_SECONDS = 60L

        private val SU_PATHS =
            listOf(
                "/system/bin/su",
                "/system/xbin/su",
                "/sbin/su",
                "/su/bin/su",
                "/magisk/.core/bin/su",
                "/data/adb/magisk/su",
                "/data/adb/ksu/bin/su",
                "/data/adb/ap/su",
            )
    }
}
