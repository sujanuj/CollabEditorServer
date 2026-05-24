package com.collabedit.server

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Manages all active editing sessions on the server.
 * Thread-safe — multiple WebSocket connections access this concurrently.
 */
class SessionManager {

    private val sessions = mutableMapOf<String, EditorSession>()
    private val mutex = Mutex()

    /**
     * Get an existing session or create a new one.
     */
    suspend fun getOrCreateSession(sessionId: String): EditorSession = mutex.withLock {
        sessions.getOrPut(sessionId) {
            println("[SessionManager] Creating new session: $sessionId")
            EditorSession(sessionId)
        }
    }

    /**
     * Get a session if it exists, null otherwise.
     */
    suspend fun getSession(sessionId: String): EditorSession? = mutex.withLock {
        sessions[sessionId]
    }

    /**
     * Remove a session if it's empty (no clients left).
     */
    suspend fun cleanupIfEmpty(sessionId: String) = mutex.withLock {
        val session = sessions[sessionId]
        if (session != null && session.isEmpty()) {
            sessions.remove(sessionId)
            println("[SessionManager] Removed empty session: $sessionId")
        }
    }

    /**
     * Returns how many active sessions exist — useful for monitoring.
     */
    suspend fun activeSessionCount(): Int = mutex.withLock {
        sessions.size
    }

    /**
     * Returns how many total clients are connected across all sessions.
     */
    suspend fun totalClientCount(): Int = mutex.withLock {
        sessions.values.sumOf { it.getClientCount() }
    }
}