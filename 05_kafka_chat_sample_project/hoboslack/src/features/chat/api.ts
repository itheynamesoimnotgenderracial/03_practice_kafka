import { API_BASE_URL } from "#/lib/constants";
import z from "zod";
import type { ChatMessage } from "./type";
import { createServerFn } from "@tanstack/react-start";

const getMessageSchema = z.object({
    roomId: z.string().min(1, "roomId is required"),
    limit: z.number().int().positive().optional().default(30),
    before: z.number().int().positive().optional(),
    token: z.string(),
})

const sendMessageSchema = z.object({
    roomId: z.string().min(1, "roomId is required"),
    content: z.string().min(1, "content must not be empty").max(4096, "content exceed max-length"),
    userId: z.string().min(1, "userId is required"),
    token: z.string(),
})

export const getMessages = createServerFn({method: "GET"})
    .inputValidator(getMessageSchema)
    .handler(async ({ data }) => {
        const params = new URLSearchParams({
            roomId: data.roomId,
            limit: String(data.limit)
        })

        if(data.before !== undefined) {
            params.set("before", String(data.before))
        }

        const res = await fetch(`${API_BASE_URL}api/messages?${params.toString()}`, {
            headers: {
                "Authorization": `Bearer ${data.token}`
            }
        })

        if(!res.ok) {
            throw new Error(`Failed to fetch messages: ${res.status} ${res.statusText}`)
        }

        const message: ChatMessage[] | null = await res.json()

        return message ?? []
})

export const sendMessage = createServerFn({ method: "POST" })
    .inputValidator(sendMessageSchema)
    .handler(async ({ data }) => {
    const res = await fetch(`${API_BASE_URL}api/messages`, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${data.token}`,
        },
        body: JSON.stringify({
            room_id: data.roomId,
            content: data.content
        }),
    })

    if (!res.ok) {
        const errorBody = await res.json().catch(() => ({}))
        throw new Error(
            (errorBody as { error?: string }).error ?? `Failed to send message: ${res.status}`
        )
    }

    return (await res.json() as { status: string; message_id: string })
})
