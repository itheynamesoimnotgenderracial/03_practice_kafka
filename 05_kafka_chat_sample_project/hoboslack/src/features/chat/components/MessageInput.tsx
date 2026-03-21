import { useState, useCallback } from 'react'
import {
  Box,
  TextField,
  IconButton,
  Typography,
  Stack,
  Fade,
} from '@mui/material'
import { glass } from '#/styles/theme'

interface MessageInputProps {
  onSend: (content: string) => void
  isPending: boolean
  isError: boolean
  error: Error | null
  onReset: () => void
}

export function MessageInput({
  onSend,
  isPending,
  isError,
  error,
  onReset,
}: MessageInputProps) {
  const [value, setValue] = useState('')

  const handleSend = useCallback(() => {
    const trimmed = value.trim()
    if (trimmed && !isPending) {
      onSend(trimmed)
      setValue('')
    }
  }, [value, isPending, onSend])

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault()
        handleSend()
      }
    },
    [handleSend],
  )

  return (
    <Box
      sx={{
        px: 2,
        py: 1.5,
        borderTop: `1px solid ${glass.border.subtle}`,
        backgroundColor: glass.surface.primary,
        backdropFilter: glass.blur.md,
      }}
    >
      {/* Error banner */}
      <Fade in={isError}>
        <Box>
          {isError && (
            <Stack
              direction="row"
              alignItems="center"
              justifyContent="space-between"
              sx={{
                mb: 1,
                px: 1.5,
                py: 0.75,
                borderRadius: 2,
                backgroundColor: 'rgba(255, 77, 106, 0.10)',
                border: '1px solid rgba(255, 77, 106, 0.25)',
              }}
            >
              <Typography
                variant="caption"
                sx={{ color: 'error.main', fontSize: '0.7rem' }}
              >
                Failed to send: {error?.message ?? 'Unknown error'}
              </Typography>
              <Typography
                component="button"
                variant="caption"
                onClick={onReset}
                sx={{
                  color: 'error.light',
                  fontSize: '0.7rem',
                  fontWeight: 600,
                  cursor: 'pointer',
                  border: 'none',
                  background: 'none',
                  '&:hover': { textDecoration: 'underline' },
                }}
              >
                Dismiss
              </Typography>
            </Stack>
          )}
        </Box>
      </Fade>

      {/* Input row */}
      <Stack direction="row" spacing={1} alignItems="flex-end">
        <TextField
          fullWidth
          multiline
          maxRows={4}
          placeholder="Type a message..."
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={isPending}
          sx={{
            '& .MuiOutlinedInput-root': {
              borderRadius: '14px',
              fontSize: '0.875rem',
            },
          }}
        />
        <IconButton
          onClick={handleSend}
          disabled={!value.trim() || isPending}
          sx={{
            width: 42,
            height: 42,
            borderRadius: '12px',
            flexShrink: 0,
            background:
              value.trim() && !isPending
                ? 'linear-gradient(135deg, #00E0FF 0%, #8B5CF6 100%)'
                : glass.surface.primary,
            border: `1px solid ${
              value.trim() && !isPending
                ? 'transparent'
                : glass.border.default
            }`,
            color: value.trim() && !isPending ? '#000' : 'text.disabled',
            transition: 'all 0.2s ease',
            '&:hover': {
              background:
                value.trim() && !isPending
                  ? 'linear-gradient(135deg, #66EEFF 0%, #A78BFA 100%)'
                  : glass.surface.hover,
              boxShadow:
                value.trim() && !isPending ? glass.glow.cyan : 'none',
              transform: 'translateY(-1px)',
            },
            '&.Mui-disabled': {
              color: 'text.disabled',
            },
          }}
        >
          <svg
            width="18"
            height="18"
            viewBox="0 0 24 24"
            fill="currentColor"
          >
            <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z" />
          </svg>
        </IconButton>
      </Stack>

      {/* Hint */}
      <Typography
        variant="caption"
        sx={{
          display: 'block',
          mt: 0.75,
          px: 0.5,
          color: 'text.disabled',
          fontSize: '0.6rem',
        }}
      >
        Press Enter to send, Shift+Enter for new line
      </Typography>
    </Box>
  )
}