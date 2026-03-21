import { useCallback, useEffect, useRef, useState } from "react"

type WebsocketStatus = "connecting" | "connected" | "disconnected" | "reconnecting"

interface UseWebsocketOptions {
    url: string
    onMessage?: (data: string) => void
    onOpen?: () => void
    onClose?: (event: CloseEvent) => void
    onError?: (event: Event) => void
    reconnect?: boolean
    maxRetries?: number
    baseDelay?: number
    maxDelay?: number
    enabled?: boolean
}

interface UseWebsocketReturn {
    status: WebsocketStatus
    send: (data: string) => void
    close: () => void
    reconnect: () => void
}

export function useWebsocket({
    url,
    onMessage,
    onOpen,
    onClose,
    onError,
    reconnect: shouldReconnect = true,
    maxRetries = 10,
    baseDelay = 1000,
    maxDelay = 30000,
    enabled = true
}: UseWebsocketOptions): UseWebsocketReturn {
    const [status, setStatus] = useState<WebsocketStatus>("disconnected")
    const wsRef = useRef<WebSocket | null>(null)
    const retriesRef = useRef(0)
    const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
    const intentionalCloseRef = useRef(false)

    const onMessageRef = useRef(onMessage)
    const onOpenRef = useRef(onOpen)
    const onCloseRef = useRef(onClose)
    const onErrorRef = useRef(onError)

    onMessageRef.current = onMessage
    onOpenRef.current = onOpen
    onCloseRef.current = onClose
    onErrorRef.current = onError

    const clearReconnectTimer = useCallback(() => {
        if(reconnectTimerRef.current) {
            clearTimeout(reconnectTimerRef.current)
            reconnectTimerRef.current = null
        }
    }, [])

    const connect = useCallback(() => {
        if(wsRef.current) {
            wsRef.current.onopen = null
            wsRef.current.onclose = null
            wsRef.current.onmessage = null
            wsRef.current.onerror = null

            if(
                wsRef.current.readyState === WebSocket.OPEN ||
                wsRef.current.readyState === WebSocket.CONNECTING
            ) {
                wsRef.current.close()
            }
        }

        setStatus("connecting")
        intentionalCloseRef.current = false

        const socket = new WebSocket(url)
        wsRef.current = socket

        socket.onopen = () => {
            setStatus("connected")
            retriesRef.current = 0
            onOpenRef.current?.()
        }

        socket.onmessage = (event) => {
            onMessageRef.current?.(event.data)
        }

        socket.onerror = (event) => {
            onErrorRef.current?.(event)
        }

        socket.onclose = (event) => {
            setStatus("disconnected")
            onCloseRef.current?.(event)

            if(!intentionalCloseRef.current && shouldReconnect && retriesRef.current < maxRetries) {
                const delay = Math.min(baseDelay * Math.pow(2, retriesRef.current), maxDelay)
                retriesRef.current += 1
                setStatus("reconnecting")

                console.log(
                `[WebSocket] Reconnecting in ${delay}ms (attempt ${retriesRef.current}/${maxRetries})`,
                )

                reconnectTimerRef.current = setTimeout(() => {
                    connect()
                }, delay)
            }
        }
    }, [url, shouldReconnect, maxRetries, baseDelay, maxDelay])

    const send = useCallback((data: string) => {
        if(wsRef.current?.readyState === WebSocket.OPEN) {
            wsRef.current.send(data)
        } else {
            console.warn("[WebSocket] Cannot send — not connected")
        }
    }, [])

    const close = useCallback(() => {
        intentionalCloseRef.current = true
        clearReconnectTimer()
        if(wsRef.current) {
            wsRef.current.close()
        }
        setStatus("disconnected")
    }, [clearReconnectTimer])

    const manualReconnect = useCallback(() => {
        clearReconnectTimer()
        retriesRef.current = 0
        connect()
    }, [connect, clearReconnectTimer])

    useEffect(() => {
        if(!enabled) {
            return
        }

        connect()

        return () => {
            intentionalCloseRef.current = true
            clearReconnectTimer()
            if(wsRef.current) {
                wsRef.current.close()
            }
        }
    }, [url, enabled, connect, clearReconnectTimer])

    return {
        status,
        send,
        close,
        reconnect: manualReconnect
    }
}