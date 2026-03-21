export type ChatMessage = {
  room_id: string
  sequence: number
  user_id: string
  message_id: string
  content: string
  timestamp: number
}

export type SendMessageRequest = {
    room_id: string;
    message_id: string;
}

export type SendMessageResponse = {
    status: string;
    message_id: string;
}

export type GetMessagesParams = {
    roomId: string;
    limit?: number;
    before?: number;
}

export type OptimisticMessage = ChatMessage & {
    _optimistic: true;
    _tempId: string;
}

export function isOptimistic(
    msg: ChatMessage | OptimisticMessage,
): msg is OptimisticMessage {
    return "_optimistic" in msg && msg._optimistic == true
}

export interface UseSendMessageOptions {
  roomId: string
  userId: string
}