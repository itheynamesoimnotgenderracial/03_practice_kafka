import { Box, Typography, Stack } from '@mui/material'
import type { ChatMessage, OptimisticMessage } from '../type'
import { isOptimistic } from '../type'
import { glass } from '#/styles/theme'

interface MessageItemProps {
  message: ChatMessage | OptimisticMessage
  isOwn: boolean
}

function formatTime(timestamp: number): string {
  const date = new Date(timestamp * 1000)
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function userColor(userId: string): string {
  const colors = [
    '#00E0FF',
    '#FF00C8',
    '#8B5CF6',
    '#34D399',
    '#FBBF24',
    '#FF4D6A',
    '#A78BFA',
    '#6EE7B7',
  ]
  let hash = 0
  for (let i = 0; i < userId.length; i++) {
    hash = userId.charCodeAt(i) + ((hash << 5) - hash)
  }
  return colors[Math.abs(hash) % colors.length]
}

function userInitial(userId: string): string {
  return userId.charAt(0).toUpperCase()
}

export function MessageItem({ message, isOwn }: MessageItemProps) {
  const optimistic = isOptimistic(message)
  const color = userColor(message.user_id)

  return (
    <Stack
      direction="row"
      spacing={1.5}
      sx={{
        justifyContent: isOwn ? 'flex-end' : 'flex-start',
        opacity: optimistic ? 0.6 : 1,
        transform: optimistic ? "translateY(4px)" : "translateY(0)",
        transition: 'opacity 0.3s ease, transform 0.3s ease',
        animation: "messageSlideIn 0.2s ease-out",
        "@keyframes messageSlideIn": {
          from: {
            opacity: 0,
            transform: "translateY(8px)",
          },
          to: {
            opacity: optimistic ? 0.6 : 1,
            transform: "translateY(0)"
          }
        }
      }}
    >
      {/* Avatar — left side for others */}
      {!isOwn && (
        <Box
          sx={{
            width: 32,
            height: 32,
            borderRadius: '10px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.75rem',
            fontWeight: 700,
            flexShrink: 0,
            mt: 0.5,
            background: `linear-gradient(135deg, ${color}30, ${color}15)`,
            border: `1px solid ${color}40`,
            color: color,
          }}
        >
          {userInitial(message.user_id)}
        </Box>
      )}

      {/* Bubble */}
      <Box sx={{ maxWidth: '70%', minWidth: 0 }}>
        {/* Username + time */}
        <Stack
          direction="row"
          spacing={1}
          alignItems="baseline"
          sx={{
            mb: 0.5,
            justifyContent: isOwn ? 'flex-end' : 'flex-start',
          }}
        >
          {!isOwn && (
            <Typography
              variant="caption"
              sx={{ fontWeight: 600, color: color, fontSize: '0.7rem' }}
            >
              {message.user_id}
            </Typography>
          )}
          <Typography
            variant="caption"
            sx={{ color: 'text.disabled', fontSize: '0.6rem' }}
          >
            {optimistic ? (message as OptimisticMessage)._failed ? "⚠ Failed to send" : "Sending..." : formatTime(message.timestamp)}
          </Typography>
        </Stack>

        {/* Message content */}
        <Box
          sx={{
            px: 2,
            py: 1.25,
            borderRadius: isOwn ? '14px 14px 4px 14px' : '14px 14px 14px 4px',
            backgroundColor: isOwn
              ? 'rgba(0, 224, 255, 0.10)'
              : glass.surface.elevated,
            backdropFilter: glass.blur.sm,
            border: isOptimistic(message) && (message as OptimisticMessage)._failed 
            ? "1px solid rgba(255, 77, 106, 0.5)" 
            : isOwn 
              ? "1px solid rgba(0, 224, 255, 0.15)"
              : `1px solid ${glass.border.subtle}`,
            ...(optimistic && {
              borderStyle: 'dashed',
            }),
          }}
        >
          <Typography
            variant="body2"
            sx={{
              wordBreak: 'break-word',
              whiteSpace: 'pre-wrap',
              lineHeight: 1.5,
            }}
          >
            {message.content}
          </Typography>
        </Box>
      </Box>

      {/* Avatar — right side for own */}
      {isOwn && (
        <Box
          sx={{
            width: 32,
            height: 32,
            borderRadius: '10px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '0.75rem',
            fontWeight: 700,
            flexShrink: 0,
            mt: 0.5,
            background: 'linear-gradient(135deg, rgba(0,224,255,0.25), rgba(139,92,246,0.20))',
            border: '1px solid rgba(0,224,255,0.35)',
            color: '#00E0FF',
          }}
        >
          {userInitial(message.user_id)}
        </Box>
      )}
    </Stack>
  )
}