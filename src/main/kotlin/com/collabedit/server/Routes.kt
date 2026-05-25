package com.collabedit.server

import com.google.gson.Gson
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*

private val gson = Gson()

fun Routing.syncRoutes(sessionManager: SessionManager) {
    webSocket("/session/{sessionId}") {
        val sessionId = call.parameters["sessionId"] ?: run {
            close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "Missing sessionId"))
            return@webSocket
        }

        val session = sessionManager.getOrCreateSession(sessionId)
        var clientSiteId: String? = null

        println("[Routes] New WebSocket connection to session: $sessionId")

        try {
            for (frame in incoming) {
                if (frame !is Frame.Text) continue
                val rawMessage = frame.readText()

                try {
                    val json = JsonParser.parseString(rawMessage).asJsonObject
                    val type = json.get("type")?.asString ?: continue

                    when (type) {

                        MessageType.JOIN -> {
                            val siteId = json.get("siteId").asString
                            val userName = json.get("userName").asString
                            val userColor = json.get("userColor").asString
                            clientSiteId = siteId

                            // Get existing clients BEFORE adding the new one
                            val existingClients = session.getExistingClients(excludeSiteId = siteId)

                            val client = Client(
                                siteId = siteId,
                                userName = userName,
                                userColor = userColor,
                                session = this
                            )
                            session.addClient(client)

                            // Get history and sort by clock for causal ordering
                            val history = session.getHistory()
                            val sortedHistory = history.sortedBy { opJson ->
                                try {
                                    JsonParser.parseString(opJson)
                                        .asJsonObject
                                        .get("clock")?.asLong ?: 0L
                                } catch (e: Exception) { 0L }
                            }

                            val historyResponse = JsonObject().apply {
                                addProperty("type", MessageType.HISTORY)
                                add("operations", gson.toJsonTree(sortedHistory))
                                addProperty("sessionId", sessionId)
                                addProperty("clientCount", session.getClientCount())
                            }
                            send(Frame.Text(gson.toJson(historyResponse)))

                            // Tell new client about everyone already in the session
                            existingClients.forEach { existingClient ->
                                val existingUserMsg = JsonObject().apply {
                                    addProperty("type", MessageType.JOIN)
                                    addProperty("siteId", existingClient.siteId)
                                    addProperty("userName", existingClient.userName)
                                    addProperty("userColor", existingClient.userColor)
                                }
                                send(Frame.Text(gson.toJson(existingUserMsg)))
                            }

                            // Tell everyone else that this person joined
                            val joinNotice = JsonObject().apply {
                                addProperty("type", MessageType.JOIN)
                                addProperty("siteId", siteId)
                                addProperty("userName", userName)
                                addProperty("userColor", userColor)
                            }
                            session.broadcast(gson.toJson(joinNotice), excludeSiteId = siteId)

                            println("[Routes] $userName joined session $sessionId")
                        }

                        MessageType.INSERT, MessageType.DELETE -> {
                            session.addToHistory(rawMessage)
                            session.broadcast(rawMessage, excludeSiteId = clientSiteId)
                        }

                        MessageType.CURSOR -> {
                            session.broadcast(rawMessage, excludeSiteId = clientSiteId)
                        }

                        MessageType.SYNC_REQUEST -> {
                            val vectorClock = json.getAsJsonObject("vectorClock")
                            val history = session.getHistory()
                            val missingOps = history.filter { opJson ->
                                try {
                                    val op = JsonParser.parseString(opJson).asJsonObject
                                    val opSiteId = op.get("siteId")?.asString ?: return@filter false
                                    val opClock = op.get("clock")?.asLong ?: return@filter false
                                    val clientHighest = vectorClock?.get(opSiteId)?.asLong ?: 0L
                                    opClock > clientHighest
                                } catch (e: Exception) { false }
                            }.sortedBy { opJson ->
                                try {
                                    JsonParser.parseString(opJson)
                                        .asJsonObject
                                        .get("clock")?.asLong ?: 0L
                                } catch (e: Exception) { 0L }
                            }
                            val syncResponse = JsonObject().apply {
                                addProperty("type", MessageType.SYNC_RESPONSE)
                                add("operations", gson.toJsonTree(missingOps))
                            }
                            send(Frame.Text(gson.toJson(syncResponse)))
                        }

                        else -> println("[Routes] Unknown message type: $type")
                    }

                } catch (e: Exception) {
                    println("[Routes] Error processing message: ${e.message}")
                }
            }

        } catch (e: Exception) {
            println("[Routes] WebSocket error for $clientSiteId: ${e.message}")
        } finally {
            clientSiteId?.let { siteId ->
                session.removeClient(siteId)
                val leaveNotice = JsonObject().apply {
                    addProperty("type", MessageType.LEAVE)
                    addProperty("siteId", siteId)
                }
                session.broadcast(gson.toJson(leaveNotice))
            }
            sessionManager.cleanupIfEmpty(sessionId)
            println("[Routes] Connection closed for $clientSiteId in session $sessionId")
        }
    }
}

fun Routing.healthRoute() {
    get("/health") {
        call.respondText(
            text = """{"status":"ok","server":"CollabEditor"}""",
            contentType = ContentType.Application.Json,
            status = HttpStatusCode.OK
        )
    }
}