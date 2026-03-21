import { Box, Typography, Stack, IconButton, Chip } from '@mui/material'
import { Link } from '@tanstack/react-router'
import { glass } from '#/styles/theme'

interface RoomHeaderProps {
  roomId: string
  wsStatus: string
}

export function RoomHeader({ roomId, wsStatus }: RoomHeaderProps) {
  const statusColor =
    wsStatus === 'connected'
      ? '#34D399'
      : wsStatus === 'connecting' || wsStatus === 'reconnecting'
        ? '#FBBF24'
        : '#FF4D6A'

  const statusLabel =
    wsStatus === 'connected'
      ? 'Live'
      : wsStatus === 'connecting'
        ? 'Connecting...'
        : wsStatus === 'reconnecting'
          ? 'Reconnecting...'
          : 'Offline'

  return (
    <Box
      sx={{
        px: 2,
        py: 1.5,
        display: 'flex',
        alignItems: 'center',
        gap: 1.5,
        borderBottom: `1px solid ${glass.border.subtle}`,
        backgroundColor: glass.surface.primary,
        backdropFilter: glass.blur.lg,
      }}
    >
      {/* Back button */}
      <IconButton
        component={Link}
        to="/rooms"
        size="small"
        sx={{
          width: 36,
          height: 36,
          borderRadius: '10px',
          border: `1px solid ${glass.border.default}`,
          color: 'text.secondary',
          '&:hover': {
            borderColor: glass.border.accent,
            color: 'primary.main',
            backgroundColor: glass.surface.hover,
          },
        }}
      >
        <svg
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <path d="M15 18l-6-6 6-6" />
        </svg>
      </IconButton>

      {/* Room info */}
      <Stack sx={{ flex: 1, minWidth: 0 }}>
        <Typography
          variant="body2"
          sx={{
            fontWeight: 700,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {roomId}
        </Typography>
      </Stack>

      {/* Status chip */}
      <Chip
        size="small"
        label={statusLabel}
        icon={
          <Box
            sx={{
              width: 6,
              height: 6,
              borderRadius: '50%',
              bgcolor: statusColor,
              boxShadow: `0 0 6px ${statusColor}`,
              ml: 1,
            }}
          />
        }
        sx={{
          height: 26,
          fontSize: '0.65rem',
          fontWeight: 600,
          backgroundColor: `${statusColor}15`,
          borderColor: `${statusColor}30`,
          color: statusColor,
          border: `1px solid`,
          '& .MuiChip-icon': {
            margin: 0,
          },
        }}
      />
    </Box>
  )
}