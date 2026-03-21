import { createFileRoute } from '@tanstack/react-router'
import { Box, Paper } from '@mui/material'
import { RoomHeader } from '#/features/chat/components/RoomHeader'
import { MessageList } from '#/features/chat/components/MessageList'
import { MessageInput } from '#/features/chat/components/MessageInput'
import {
  useMessages,
  useSendMessage,
  useAppendMessage,
} from '#/features/chat/hooks'
import { useRoomSocket } from '#/hooks/use-room-socket'

export const Route = createFileRoute('/rooms/$roomId')({
  component: ChatRoom,
})

// Temporary user ID — in production this comes from auth
const USER_ID = 'user-1'

function ChatRoom() {
  const { roomId } = Route.useParams()

  // ── Step 4: Message history with infinite scroll ──
  const {
    data,
    hasNextPage,
    isFetchingNextPage,
    fetchNextPage,
    isLoading,
  } = useMessages(roomId)

  // ── Step 5: Send messages ──
  const { send, isPending, isError, error, reset } = useSendMessage({
    roomId,
    userId: USER_ID,
  })

  // ── Step 6: Real-time incoming messages via WebSocket ──
  const appendMessage = useAppendMessage(roomId)
  const { status: wsStatus } = useRoomSocket({
    roomId,
    onMessage: appendMessage,
  })

  return (
    <Box
      sx={{
        height: '100vh',
        display: 'flex',
        flexDirection: 'column',
        maxWidth: 800,
        mx: 'auto',
      }}
    >
      <Paper
        sx={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          borderRadius: { xs: 0, sm: '20px' },
          my: { xs: 0, sm: 2 },
          mx: { xs: 0, sm: 2 },
          overflow: 'hidden',
          minHeight: 0, // critical for flex child scroll
        }}
      >
        {/* Header — Step 4 */}
        <RoomHeader roomId={roomId} wsStatus={wsStatus} />

        {/* Message list — Step 4 (infinite scroll) + Step 6 (live updates) */}
        <MessageList
          pages={data?.pages}
          hasNextPage={hasNextPage ?? false}
          isFetchingNextPage={isFetchingNextPage}
          fetchNextPage={fetchNextPage}
          isLoading={isLoading}
          userId={USER_ID}
        />

        {/* Input — Step 5 */}
        <MessageInput
          onSend={send}
          isPending={isPending}
          isError={isError}
          error={error}
          onReset={reset}
        />
      </Paper>
    </Box>
  )
}