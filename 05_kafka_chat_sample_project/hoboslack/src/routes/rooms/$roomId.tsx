import { createFileRoute } from '@tanstack/react-router'
import { Box, Paper, Snackbar, Alert, CircularProgress, Typography } from '@mui/material'
import { RoomHeader } from '#/features/chat/components/RoomHeader'
import { MessageList } from '#/features/chat/components/MessageList'
import { MessageInput } from '#/features/chat/components/MessageInput'
import { useMessages, useSendMessage, useAppendMessage, chatKeys } from '#/features/chat/hooks'
import { useRoomSocket } from '#/hooks/use-room-socket'
import { getUserId } from '#/lib/identity'
import { getMessages } from '#/features/chat'
import { useMemo } from 'react'

export const Route = createFileRoute('/rooms/$roomId')({
  loader: async ({ params, context }) => {
    const { roomId } = params

    await context.queryClient.prefetchInfiniteQuery({
      queryKey: chatKeys.messages(roomId),
      queryFn: () => getMessages({ data: { roomId, limit: 30 } }),
      initialPageParam: undefined,
      getNextPageParam: (_lastpage, _allPages) => {

      },
      pages: 1,
    })
  },
  pendingComponent: () => (
    <Box sx={{ height: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
      <CircularProgress size={28} sx={{ color: 'primary.main' }} />
    </Box>
  ),
  errorComponent: ({ error }) => (
    <Box sx={{ height: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', flexDirection: 'column', gap: 2 }}>
      <Typography variant="body1" sx={{ color: 'error.main' }}>
        Failed to load room
      </Typography>
      <Typography variant="caption" sx={{ color: 'text.secondary' }}>
        {error instanceof Error ? error.message : 'Unknown error'}
      </Typography>
    </Box>
  ),
  component: ChatRoom,
})

function ChatRoom() {
  const { roomId } = Route.useParams()
  const userId = useMemo(() => getUserId(), [])  // ← Chapter 14: stable identity per browser

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