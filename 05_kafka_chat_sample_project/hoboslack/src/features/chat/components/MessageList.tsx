import { useRef, useEffect, useCallback, useMemo } from 'react'
import { Box, Typography, CircularProgress, Button, Stack, Skeleton } from '@mui/material'
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
          overflowY: 'auto',
          px: 2,
          py: 2,
          display: 'flex',
          flexDirection: 'column',
          gap: 1.5,
        }}
      >
        {/* Simulate a mix of own and other messages */}
        {[
          { isOwn: false, width: '55%' },
          { isOwn: false, width: '40%' },
          { isOwn: true,  width: '45%' },
          { isOwn: false, width: '60%' },
          { isOwn: true,  width: '35%' },
          { isOwn: true,  width: '50%' },
          { isOwn: false, width: '42%' },
          { isOwn: true,  width: '38%' },
        ].map((item, i) => (
          <Box
            key={i}
            sx={{
              display: 'flex',
              justifyContent: item.isOwn ? 'flex-end' : 'flex-start',
              alignItems: 'flex-end',
              gap: 1,
            }}
          >
            {/* Avatar skeleton for others */}
            {!item.isOwn && (
              <Skeleton
                variant="rounded"
                width={32}
                height={32}
                sx={{ borderRadius: '10px', flexShrink: 0 }}
              />
            )}

            {/* Bubble skeleton */}
            <Box sx={{ maxWidth: '70%', width: item.width }}>
              {/* Username line for others */}
              {!item.isOwn && (
                <Skeleton
                  variant="text"
                  width="40%"
                  sx={{ fontSize: '0.7rem', mb: 0.5 }}
                />
              )}
              <Skeleton
                variant="rounded"
                width="100%"
                height={40}
                sx={{
                  borderRadius: item.isOwn
                    ? '14px 14px 4px 14px'
                    : '14px 14px 14px 4px',
                }}
              />
            </Box>
          </Box>
        ))}
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