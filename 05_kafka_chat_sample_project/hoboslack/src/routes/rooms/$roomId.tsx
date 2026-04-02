import { createFileRoute } from '@tanstack/react-router'
import { Box, Paper, Snackbar, Alert } from '@mui/material'
import { RoomHeader } from '#/features/chat/components/RoomHeader'
import { MessageList } from '#/features/chat/components/MessageList'
import { MessageInput } from '#/features/chat/components/MessageInput'
import { useMessages, useSendMessage, useAppendMessage } from '#/features/chat/hooks'
import { useRoomSocket } from '#/hooks/use-room-socket'
import { getUserId } from '#/lib/identity'

export const Route = createFileRoute('/rooms/$roomId')({
  component: ChatRoom,
})

function ChatRoom() {
  const { roomId } = Route.useParams()
  const userId = getUserId()  // ← Chapter 14: stable identity per browser

  const { data, hasNextPage, isFetchingNextPage, fetchNextPage, isLoading } =
    useMessages(roomId)

  const { send, isPending, isError, error, reset } = useSendMessage({
    roomId,
    userId,
  })

  const appendMessage = useAppendMessage(roomId)
  const { status: wsStatus } = useRoomSocket({
    roomId,
    onMessage: appendMessage,
  })

  // Chapter 15: block send when WS is disconnected
  const isOffline = wsStatus === 'disconnected'

  return (
    <Box sx={{ height: '100vh', display: 'flex', flexDirection: 'column', maxWidth: 800, mx: 'auto' }}>
      <Paper
        sx={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          borderRadius: { xs: 0, sm: '20px' },
          my: { xs: 0, sm: 2 },
          mx: { xs: 0, sm: 2 },
          overflow: 'hidden',
          minHeight: 0,
        }}
      >
        <RoomHeader roomId={roomId} wsStatus={wsStatus} />

        <MessageList
          pages={data?.pages}
          hasNextPage={hasNextPage ?? false}
          isFetchingNextPage={isFetchingNextPage}
          fetchNextPage={fetchNextPage}
          isLoading={isLoading}
          userId={userId}
        />

        <MessageInput
          onSend={send}
          isPending={isPending}
          isError={isError}
          error={error}
          onReset={reset}
          disabled={isOffline}  // ← Chapter 15: disable input when offline
        />
      </Paper>

      {/* Chapter 15: offline notification banner */}
      <Snackbar
        open={isOffline}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
      >
        <Alert severity="warning" sx={{ width: '100%' }}>
          Disconnected — reconnecting...
        </Alert>
      </Snackbar>
    </Box>
  )
}