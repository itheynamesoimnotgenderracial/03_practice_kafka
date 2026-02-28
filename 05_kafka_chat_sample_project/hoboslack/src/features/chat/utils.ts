export type ChatMessage = {
  room_id: string
  sequence: number
  user_id: string
  message_id: string
  content: string
  timestamp: number
}