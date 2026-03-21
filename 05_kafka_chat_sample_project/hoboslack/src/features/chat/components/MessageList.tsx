import { useRef, useEffect, useCallback, useMemo } from 'react'
import { Box, Typography, CircularProgress, Button, Stack } from '@mui/material'
import {MessageItem} from './MessageItem'
import { flattenMessages } from '../hooks'
import type { ChatMessage } from '../type'
import { glass } from '#/styles/theme'

interface MessageListProps {
  pages: ChatMessage[][] | undefined
  hasNextPage: boolean
  isFetchingNextPage: boolean
  fetchNextPage: () => void
  isLoading: boolean
  userId: string
}

export function MessageList({
  pages,
  hasNextPage,
  isFetchingNextPage,
  fetchNextPage,
  isLoading,
  userId,
}: MessageListProps) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const bottomRef = useRef<HTMLDivElement>(null)
  const isNearBottomRef = useRef(true)
  const prevMessageCountRef = useRef(0)

  const messages = useMemo(() => flattenMessages(pages), [pages])

  // Track if user is near bottom
  const handleScroll = useCallback(() => {
    const el = scrollRef.current
    if (!el) return

    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight
    isNearBottomRef.current = distanceFromBottom < 80

    // Load older messages when scrolling near top
    if (el.scrollTop < 60 && hasNextPage && !isFetchingNextPage) {
      fetchNextPage()
    }
  }, [hasNextPage, isFetchingNextPage, fetchNextPage])

  // Auto-scroll to bottom when new messages arrive (if user was near bottom)
  useEffect(() => {
    if (messages.length > prevMessageCountRef.current) {
      if (isNearBottomRef.current) {
        bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
      }
    }
    prevMessageCountRef.current = messages.length
  }, [messages.length])

  // Scroll to bottom on initial load
  useEffect(() => {
    if (!isLoading && messages.length > 0) {
      bottomRef.current?.scrollIntoView()
    }
  }, [isLoading]) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Loading state ──
  if (isLoading) {
    return (
      <Box
        sx={{
          flex: 1,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <Stack alignItems="center" spacing={2}>
          <CircularProgress
            size={28}
            sx={{ color: 'primary.main' }}
          />
          <Typography variant="body2" sx={{ color: 'text.secondary' }}>
            Loading messages...
          </Typography>
        </Stack>
      </Box>
    )
  }

  // ── Empty state ──
  if (messages.length === 0) {
    return (
      <Box
        sx={{
          flex: 1,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <Stack alignItems="center" spacing={2}>
          <Box
            sx={{
              width: 56,
              height: 56,
              borderRadius: '50%',
              background: glass.surface.elevated,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <svg
              width="24"
              height="24"
              viewBox="0 0 24 24"
              fill="none"
              stroke="rgba(255,255,255,0.25)"
              strokeWidth={1.5}
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
            </svg>
          </Box>
          <Typography variant="body2" sx={{ color: 'text.secondary' }}>
            No messages yet. Start the conversation!
          </Typography>
        </Stack>
      </Box>
    )
  }

  // ── Message list ──
  return (
    <Box
      ref={scrollRef}
      onScroll={handleScroll}
      sx={{
        flex: 1,
        overflowY: 'auto',
        px: 2,
        py: 2,
        display: 'flex',
        flexDirection: 'column',
        gap: 1.5,
      }}
    >
      {/* Load more indicator */}
      {hasNextPage && (
        <Box sx={{ textAlign: 'center', py: 1 }}>
          {isFetchingNextPage ? (
            <CircularProgress size={20} sx={{ color: 'primary.main' }} />
          ) : (
            <Button
              size="small"
              variant="text"
              onClick={fetchNextPage}
              sx={{
                fontSize: '0.7rem',
                color: 'text.secondary',
                '&:hover': { color: 'primary.main' },
              }}
            >
              Load older messages
            </Button>
          )}
        </Box>
      )}

      {messages.map((msg) => (
        <MessageItem
          key={msg.message_id}
          message={msg}
          isOwn={msg.user_id === userId}
        />
      ))}

      {/* Scroll anchor */}
      <div ref={bottomRef} />
    </Box>
  )
}