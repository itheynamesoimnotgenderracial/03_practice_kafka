import { WS_LEADERBOARD_BASE_URL } from "#/lib/constants";
import { useWebsocket } from "#/lib/websocket";
import { useCallback } from "react";

export interface LeaderboardEntry {
    room_id: string;
    total_message: number;
}

interface UseLeaderboardSocketOptions {
    windowType: "hourly" | "daily";
    onUpdate: (entries: LeaderboardEntry[]) => void;
    enabled?: boolean;
}

export function useLeaderboardSocket({
    onUpdate,
    windowType,
    enabled = true
}: UseLeaderboardSocketOptions) {
    console.log("asdasdasd =-===>", `${WS_LEADERBOARD_BASE_URL}/ws/${windowType}`)
    const url = `${WS_LEADERBOARD_BASE_URL}/ws/${windowType}`

    const handleMessage = useCallback((data: string) => {
        if(data === "refresh") return

        try {
            const entries: LeaderboardEntry[] = JSON.parse(data)
            onUpdate(entries)
        } catch(error) {
            console.error('[useLeaderboardSocket] Failed to parse:', error)
        }
    }, [onUpdate])

    const { status, close, reconnect } = useWebsocket({
        url,
        onMessage: handleMessage,
        enabled,
        onOpen: () => console.log(`[Leaderboard ${windowType}] WebSocket connected`),
        onClose: () => console.log(`[Leaderboard ${windowType}] WebSocket disconnected`),
    })

    return { status, close, reconnect }
}