package com.collabedit.server

import io.ktor.websocket.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Represents one connected client in a session.
 */
data class Client(
    val siteId: String,
    val userName: String,
    val userColor: String,
    val session: DefaultWebSocketSession
)

/**
 * Represents one active editing session.
 * Multiple clients connect to the same session to collaborate.
 */
class EditorSession(val sessionId: String) {
    private val clients = mutableMapOf<String, Client>()
    private val operationHistory = mutableListOf<String>()
    private val mutex = Mutex()

    suspend fun addClient(client: Client) = mutex.withLock {
        clients[client.siteId] = client
        println("[Session $sessionId] Client joined: ${client.userName} (${client.siteId}). Total: ${clients.size}")
    }

    suspend fun removeClient(siteId: String) = mutex.withLock {
        clients.remove(siteId)
        println("[Session $sessionId] Client left: $siteId. Remaining: ${clients.size}")
    }

    suspend fun broadcast(message: String, excludeSiteId: String? = null) = mutex.withLock {
        clients.values
            .filter { it.siteId != excludeSiteId }
            .forEach { client ->
                try {
                    client.session.send(Frame.Text(message))
                } catch (e: Exception) {
                    println("[Session $sessionId] Failed to send to ${client.siteId}: ${e.message}")
                }
            }
    }

    suspend fun addToHistory(operation: String) = mutex.withLock {
        operationHistory.add(operation)
    }

    suspend fun getHistory(): List<String> = mutex.withLock {
        operationHistory.toList()
    }

    suspend fun getClientCount(): Int = mutex.withLock {
        clients.size
    }

    suspend fun isEmpty(): Boolean = mutex.withLock {
        clients.isEmpty()
    }
}

/**
 * Message types sent between client and server.
 */
object MessageType {
    const val JOIN = "JOIN"
    const val LEAVE = "LEAVE"
    const val INSERT = "INSERT"
    const val DELETE = "DELETE"
    const val CURSOR = "CURSOR"
    const val HISTORY = "HISTORY"
    const val SYNC_REQUEST = "SYNC_REQUEST"
    const val SYNC_RESPONSE = "SYNC_RESPONSE"
    const val ERROR = "ERROR"
    const val ACK = "ACK"
}