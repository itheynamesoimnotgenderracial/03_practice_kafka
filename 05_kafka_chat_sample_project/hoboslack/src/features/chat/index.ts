// Types
export type {
  ChatMessage,
  SendMessageRequest,
  SendMessageResponse,
  GetMessagesParams,
  OptimisticMessage,
} from './type'
export { isOptimistic } from './type'

// API
export { getMessages, sendMessage } from './api'

// Hooks
export {
  chatKeys,
  useMessages,
  useSendMessage,
  useAppendMessage,
  flattenMessages,
} from './hooks'