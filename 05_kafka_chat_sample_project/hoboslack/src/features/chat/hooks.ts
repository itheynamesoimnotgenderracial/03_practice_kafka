import { useInfiniteQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { getMessages, sendMessage } from "./api";
import type { ChatMessage, OptimisticMessage, UseSendMessageOptions } from "./type";
import { useCallback } from "react";
import { v4 as uuidv4 } from 'uuid'
import { getAuthToken } from "#/lib/auth";

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
                    before: pageParam,
                    token: getAuthToken(),
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
    pages: (ChatMessage | OptimisticMessage)[][] | undefined
): (ChatMessage | OptimisticMessage)[] {
    if (!pages) return []

    const allMessages: (ChatMessage | OptimisticMessage)[] = []

    for (let i = pages.length - 1; i >= 0; --i) {
        const page = pages[i]
        for (let j = page.length - 1; j >= 0; j--) {
            allMessages.push(page[j])
        }
    }

    return allMessages
}

export function useSendMessage({ roomId, userId }: UseSendMessageOptions) {
    const queryClient = useQueryClient()
    const token = getAuthToken()
    console.log("useSendMessage ===========>", token)
    const mutation = useMutation({
        mutationFn: (vars: { content: string, tempId: string }) => {
            const content = vars.content
            return sendMessage({
                data: {
                    roomId,
                    content,
                    userId,
                    token,
                }
            })
        },
        onMutate: async({content, tempId}) => {
            // Cancel any in-flight refetches to avoid overwriting our optimistic update
            await queryClient.cancelQueries({queryKey: chatKeys.messages(roomId)})

            const optimistic: OptimisticMessage = {
                message_id: tempId,
                _tempId: tempId,
                _optimistic: true,
                room_id: roomId,
                user_id: userId,
                content: content,
                sequence: -1,
                timestamp: Math.floor(Date.now() / 1000)
            }

            queryClient.setQueryData(
                chatKeys.messages(roomId),
                (old: { pages: ChatMessage[][]; pageParams: unknown[] } | undefined) => {
                    if (!old) return old
                    const pages = [...old.pages]
                    pages[0] = [optimistic as unknown as ChatMessage, ...pages[0]]
                    return { ...old, pages }
                }
            )

            return { tempId }
        },
        onError: (_err, _vars, context) => {
            if (!context) return

            // Mark the optimistic message as failed instead of removing it
            queryClient.setQueryData(
                chatKeys.messages(roomId),
                (old: { pages: ChatMessage[][]; pageParams: unknown[] }) => {
                    if (!old) return old

                    const pages = old.pages.map((page) => page.map((msg) => {
                        const opt = msg as unknown as OptimisticMessage
                        if (opt._tempId === context.tempId) {
                            return { ...opt, _failed: true } as unknown as ChatMessage
                        }

                        return msg
                    }))
                    return { ...old, pages }
                },
            )
        },

        onSuccess: (_result, _vars, context) => {
            if (!context) return

            // remove optimistic entry - real message comes via websocket
            queryClient.setQueryData(
                chatKeys.messages(roomId),
                (old: { pages: ChatMessage[][]; pageParams: unknown[] } | undefined) => {
                    if (!old) return old
                    const pages = old.pages.map((page) => 
                        page.filter((msg) => {
                            const opt = msg as unknown as OptimisticMessage
                            return opt._tempId !== context.tempId
                        })
                    )
                    return { ...old, pages }
                }
            )
            
        }
    })

    const send = useCallback(
        (content: string) => {
            if (content.trim()) {
                mutation.mutate({content: content.trim(), tempId: uuidv4()})
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

                const exists = pages.some((page) => page.some((m) => {
                    const opt = m as unknown as OptimisticMessage
                    return m.message_id === message.message_id && !opt._optimistic
                }))
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