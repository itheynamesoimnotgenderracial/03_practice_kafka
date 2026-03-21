import { useState, useCallback } from 'react'
import { Link, createFileRoute, useNavigate } from '@tanstack/react-router'
import {
  Box,
  Typography,
  TextField,
  Button,
  Paper,
  Tabs,
  Tab,
  Chip,
  Stack,
  InputAdornment,
  Fade,
} from '@mui/material'
import {
  useLeaderboardSocket,
  type LeaderboardEntry,
} from '#/hooks/use-leaderboard-socket'
import { glass } from '#/styles/theme'

export const Route = createFileRoute('/rooms/')({
  component: RoomsIndex,
})

// ═══════════════════════════════════════════════════════════════════
// STATUS INDICATOR
// ═══════════════════════════════════════════════════════════════════

function StatusDot({ status }: { status: string }) {
  const color =
    status === 'connected'
      ? '#34D399'
      : status === 'connecting' || status === 'reconnecting'
        ? '#FBBF24'
        : '#FF4D6A'

  const label =
    status === 'connected'
      ? 'Live'
      : status === 'connecting'
        ? 'Connecting...'
        : status === 'reconnecting'
          ? 'Reconnecting...'
          : 'Offline'

  return (
    <Stack direction="row" alignItems="center" spacing={1}>
      <Box
        sx={{
          width: 8,
          height: 8,
          borderRadius: '50%',
          bgcolor: color,
          boxShadow: `0 0 8px ${color}`,
          animation:
            status === 'connected' ? 'pulse-glow 2s infinite' : 'none',
          '@keyframes pulse-glow': {
            '0%, 100%': { boxShadow: `0 0 4px ${color}` },
            '50%': { boxShadow: `0 0 12px ${color}, 0 0 24px ${color}40` },
          },
        }}
      />
      <Typography variant="overline" sx={{ color: 'text.secondary' }}>
        {label}
      </Typography>
    </Stack>
  )
}

// ═══════════════════════════════════════════════════════════════════
// RANK BADGE
// ═══════════════════════════════════════════════════════════════════

function RankBadge({ rank }: { rank: number }) {
  const styles =
    rank === 1
      ? {
          background: 'linear-gradient(135deg, #00E0FF 0%, #8B5CF6 100%)',
          color: '#000',
          boxShadow: glass.glow.cyan,
          fontWeight: 800,
        }
      : rank === 2
        ? {
            background: 'linear-gradient(135deg, #FF00C8 0%, #FF4D6A 100%)',
            color: '#000',
            boxShadow: glass.glow.magenta,
            fontWeight: 800,
          }
        : rank === 3
          ? {
              background: 'linear-gradient(135deg, #8B5CF6 0%, #A78BFA 100%)',
              color: '#000',
              boxShadow: glass.glow.violet,
              fontWeight: 800,
            }
          : {
              background: glass.surface.elevated,
              color: 'rgba(255,255,255,0.5)',
              boxShadow: 'none',
              fontWeight: 600,
            }

  return (
    <Box
      sx={{
        width: 36,
        height: 36,
        borderRadius: '10px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: '0.8rem',
        flexShrink: 0,
        ...styles,
      }}
    >
      {rank}
    </Box>
  )
}

// ═══════════════════════════════════════════════════════════════════
// LEADERBOARD ROW
// ═══════════════════════════════════════════════════════════════════

function LeaderboardRow({
  entry,
  index,
}: {
  entry: LeaderboardEntry
  index: number
}) {
  return (
    <Fade in timeout={300 + index * 60}>
      <Link
        to="/rooms/$roomId"
        params={{ roomId: entry.room_id }}
        style={{ textDecoration: 'none', color: 'inherit' }}
      >
        <Paper
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: 2,
            px: 2.5,
            py: 2,
            cursor: 'pointer',
            backgroundImage: index < 3 ? glass.mesh.card : 'none',
            '&:hover': {
              backgroundColor: glass.surface.hover,
              borderColor: glass.border.accent,
              transform: 'translateY(-2px)',
              boxShadow: glass.glow.cyan,
            },
          }}
        >
          <RankBadge rank={index + 1} />

          <Box sx={{ flex: 1, minWidth: 0 }}>
            <Typography
              variant="body2"
              sx={{
                fontWeight: 600,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {entry.room_id}
            </Typography>
          </Box>

          <Stack alignItems="flex-end" spacing={0}>
            <Typography
              variant="h6"
              sx={{
                fontWeight: 700,
                lineHeight: 1,
                background:
                  index === 0
                    ? 'linear-gradient(135deg, #00E0FF, #8B5CF6)'
                    : 'none',
                WebkitBackgroundClip: index === 0 ? 'text' : 'unset',
                WebkitTextFillColor: index === 0 ? 'transparent' : 'unset',
              }}
            >
              {entry.total_message}
            </Typography>
            <Typography variant="overline" sx={{ color: 'text.secondary' }}>
              messages
            </Typography>
          </Stack>

          <Box
            sx={{
              color: 'text.secondary',
              display: 'flex',
              transition: 'all 0.2s ease',
              '.MuiPaper-root:hover &': {
                color: 'primary.main',
                transform: 'translateX(3px)',
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
              <path d="M9 5l7 7-7 7" />
            </svg>
          </Box>
        </Paper>
      </Link>
    </Fade>
  )
}

// ═══════════════════════════════════════════════════════════════════
// MAIN PAGE
// ═══════════════════════════════════════════════════════════════════

function RoomsIndex() {
  const [dailyLeaderboard, setDailyLeaderboard] = useState<
    LeaderboardEntry[]
  >([])
  const [hourlyLeaderboard, setHourlyLeaderboard] = useState<
    LeaderboardEntry[]
  >([])
  const [activeTab, setActiveTab] = useState(0) // 0 = daily, 1 = hourly
  const [roomInput, setRoomInput] = useState('')
  const navigate = useNavigate()

  const onDailyUpdate = useCallback((entries: LeaderboardEntry[]) => {
    setDailyLeaderboard(entries)
  }, [])

  const onHourlyUpdate = useCallback((entries: LeaderboardEntry[]) => {
    setHourlyLeaderboard(entries)
  }, [])

  const { status: dailyStatus } = useLeaderboardSocket({
    windowType: 'daily',
    onUpdate: onDailyUpdate,
  })

  const { status: hourlyStatus } = useLeaderboardSocket({
    windowType: 'hourly',
    onUpdate: onHourlyUpdate,
  })

  const activeLeaderboard =
    activeTab === 0 ? dailyLeaderboard : hourlyLeaderboard
  const activeStatus = activeTab === 0 ? dailyStatus : hourlyStatus

  function handleJoinRoom(e: React.FormEvent) {
    e.preventDefault()
    const trimmed = roomInput.trim()
    if (trimmed) {
      navigate({ to: '/rooms/$roomId', params: { roomId: trimmed } })
    }
  }

  return (
    <Box
      sx={{
        maxWidth: 720,
        mx: 'auto',
        px: 3,
        pt: 8,
        pb: 10,
      }}
    >
      {/* ── HEADER ────────────────────────────────────── */}
      <Box sx={{ mb: 5 }}>
        <Chip
          label="CHAT ROOMS"
          size="small"
          color="primary"
          sx={{ mb: 2 }}
        />
        <Typography
          variant="h2"
          sx={{
            fontSize: { xs: '2.2rem', sm: '3rem' },
            background: 'linear-gradient(135deg, #00E0FF 0%, #8B5CF6 50%, #FF00C8 100%)',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent',
            mb: 1.5,
          }}
        >
          Rooms
        </Typography>
        <Typography variant="body1" sx={{ color: 'text.secondary', maxWidth: 540 }}>
          Join an existing room or create a new one. The leaderboard updates in
          real time as messages flow through the Kafka pipeline.
        </Typography>
      </Box>

      {/* ── JOIN / CREATE ─────────────────────────────── */}
      <Paper sx={{ p: 3, mb: 4 }}>
        <Typography
          variant="subtitle2"
          sx={{ color: 'primary.main', mb: 2 }}
        >
          JOIN OR CREATE
        </Typography>
        <Box
          component="form"
          onSubmit={handleJoinRoom}
          sx={{ display: 'flex', gap: 1.5 }}
        >
          <TextField
            fullWidth
            placeholder="Enter a room name or ID..."
            value={roomInput}
            onChange={(e) => setRoomInput(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <Box
                    sx={{
                      width: 8,
                      height: 8,
                      borderRadius: '50%',
                      bgcolor: 'primary.main',
                      opacity: 0.5,
                    }}
                  />
                </InputAdornment>
              ),
            }}
          />
          <Button
            type="submit"
            variant="contained"
            disabled={!roomInput.trim()}
            sx={{ whiteSpace: 'nowrap', px: 4 }}
          >
            Join Room
          </Button>
        </Box>
      </Paper>

      {/* ── LEADERBOARD ───────────────────────────────── */}
      <Paper sx={{ p: 3 }}>
        <Stack
          direction="row"
          justifyContent="space-between"
          alignItems="center"
          sx={{ mb: 3 }}
        >
          <Box>
            <Typography
              variant="subtitle2"
              sx={{ color: 'primary.main', mb: 0.5 }}
            >
              LIVE LEADERBOARD
            </Typography>
            <Typography variant="body2" sx={{ color: 'text.secondary' }}>
              Most active rooms by message count
            </Typography>
          </Box>
          <StatusDot status={activeStatus} />
        </Stack>

        {/* Tab switcher */}
        <Tabs
          value={activeTab}
          onChange={(_, v) => setActiveTab(v)}
          sx={{ mb: 3 }}
        >
          <Tab label="Daily" />
          <Tab label="Hourly" />
        </Tabs>

        {/* Room list */}
        {activeLeaderboard.length === 0 ? (
          <Box
            sx={{
              py: 8,
              textAlign: 'center',
            }}
          >
            <Box
              sx={{
                width: 48,
                height: 48,
                borderRadius: '50%',
                background: glass.surface.elevated,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                mx: 'auto',
                mb: 2,
              }}
            >
              <svg
                width="20"
                height="20"
                viewBox="0 0 24 24"
                fill="none"
                stroke="rgba(255,255,255,0.3)"
                strokeWidth={1.5}
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
              </svg>
            </Box>
            <Typography variant="body2" sx={{ color: 'text.secondary' }}>
              {activeStatus === 'connected'
                ? 'No active rooms yet. Send a message to get started!'
                : 'Waiting for leaderboard data...'}
            </Typography>
          </Box>
        ) : (
          <Stack spacing={1}>
            {activeLeaderboard.map((entry, index) => (
              <LeaderboardRow
                key={entry.room_id}
                entry={entry}
                index={index}
              />
            ))}
          </Stack>
        )}
      </Paper>
    </Box>
  )
}