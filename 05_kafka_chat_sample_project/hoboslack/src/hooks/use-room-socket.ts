import type { ChatMessage } from "#/features/chat/type";
import { getAuthToken } from "#/lib/auth";
import { WS_CHAT_BASE_URL } from "#/lib/constants";
import { useWebsocket } from "#/lib/websocket";
import { useCallback } from "react";


interface UseRoomSocketOptions {
    roomId: string;
    onMessage: (message: ChatMessage) => void
    enabled?: boolean
}

export function useRoomSocket({ roomId, onMessage, enabled = true }: UseRoomSocketOptions) {
    const token = getAuthToken()
    const url = `${WS_CHAT_BASE_URL}/ws/rooms/${roomId}?token=${token}`

    const handleMessage = useCallback((data: string) => {
        try {
            const message: ChatMessage = JSON.parse(data)
            onMessage(message)
        } catch(error) {
            console.error('[useRoomSocket] Failed to parse message:', error)
        }
    }, [onMessage])

    const { status, close, reconnect } = useWebsocket({
        url,
        onMessage: handleMessage,
        enabled: enabled && !!roomId,
        onOpen: () => console.log(`[Room ${roomId}] WebSocket connected`),
        onClose: () => console.log(`[Room ${roomId}] WebSocket disconnected`)
    })

    return { status, close, reconnect }
}