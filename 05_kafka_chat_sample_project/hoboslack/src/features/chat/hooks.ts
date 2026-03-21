import { useInfiniteQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { getMessages, sendMessage } from "./api";
import type { ChatMessage, UseSendMessageOptions } from "./type";
import { useCallback } from "react";

export const chatKeys = {
    all: ["chat"] as const,
    room: (roomId: string) => [...chatKeys.all, "room", roomId] as const,
    messages: (roomId: string) => [...chatKeys.room(roomId), "messsages"] as const,
}

const PAGE_SIZE = 30;

export function useMessages(roomId: string) {
    return useInfiniteQuery({
        queryKey: chatKeys.messages(roomId),
        queryFn: async ({ pageParam }) => {
            const message = await getMessages({
                data: {
                    roomId,
                    limit: PAGE_SIZE,
                    before: pageParam
                }
            })
            return message
        },
        initialPageParam: undefined as number | undefined,
        getNextPageParam: (lastPage) => {
            if (lastPage.length < PAGE_SIZE) return undefined

            const oldestInPage = lastPage[lastPage.length - 1]
            return oldestInPage?.sequence
        },
        enabled: !!roomId,
        staleTime: Infinity,
        refetchOnWindowFocus: false,
    })
}


export function flattenMessages(
    pages: ChatMessage[][] | undefined
): ChatMessage[] {
    if (!pages) return []

    const allMessages: ChatMessage[] = []

    for (let i = pages.length - 1; i >= 0; --i) {
        const page = pages[i]
        for (let j = page.length - 1; j >= 0; j--) {
            allMessages.push(page[j])
        }
    }

    return allMessages
}

export function useSendMessage({ roomId, userId }: UseSendMessageOptions) {
    const mutation = useMutation({
        mutationFn: (content: string) => {
            return sendMessage({
                data: {
                    roomId,
                    content,
                    userId
                }
            })
        },
        onError: (error) => {
            console.error("[useSendMessage] Failed:", error)
        }
    })

    const send = useCallback(
        (content: string) => {
            if (content.trim()) {
                mutation.mutate(content)
            }
        },
        [mutation]
    )

    return {
        send,
        isPending: mutation.isPending,
        isError: mutation.isError,
        error: mutation.error,
        reset: mutation.reset
    }
}

export function useAppendMessage(roomId: string) {
    const queryClient = useQueryClient()

    return useCallback((message: ChatMessage) => {
        queryClient.setQueryData(
            chatKeys.messages(roomId),
            (
                oldData: {
                    pages: ChatMessage[][],
                    pageParams: (number | undefined)[]
                } | undefined
            ) => {
                if(!oldData) return oldData

                const pages = [...oldData.pages]

                const exists = pages.some((page) => page.some((m) => m.message_id === message.message_id))
                if(exists) return oldData

                if(pages.length > 0) {
                    pages[0] = [message, ...pages[0]]
                } else {
                    pages.push([message])
                }

                return {
                    ...oldData,
                    pages
                }
            }
        )
    }, [queryClient, roomId])
}