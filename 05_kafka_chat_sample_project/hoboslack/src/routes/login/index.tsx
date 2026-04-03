import { createFileRoute, useNavigate, redirect } from '@tanstack/react-router'
import { useState } from 'react'
import {
  Box, Paper, TextField, Button,
  Typography, Stack, Tabs, Tab,
} from '@mui/material'
import { loginRequest, registerRequest, storeAuth, getStoredAuth, type AuthUser } from '#/lib/auth'

export const Route = createFileRoute('/login/')({
  beforeLoad: () => {
    // Already logged in → go straight to rooms
    if (getStoredAuth()) {
      throw redirect({ to: '/rooms' })
    }
  },
  component: LoginPage,
})

function LoginPage() {
  const navigate          = useNavigate()
  const [tab, setTab]     = useState(0)
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setLoading(true)

    try {
      const user = tab === 0
        ? await loginRequest({ data: { username, password } })
        : await registerRequest({ data: { username, password } })
      console.log("login user  ======>", user)
      storeAuth(user as AuthUser)
      navigate({ to: '/rooms' })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Something went wrong')
    } finally {
      setLoading(false)
    }
  }

  return (
    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', flex: 1 }}>
      <Paper sx={{ p: 4, width: '100%', maxWidth: 400 }}>
        <Typography variant="h5" sx={{ mb: 1, fontWeight: 700 }}>
          HoboSlack
        </Typography>
        <Typography variant="body2" sx={{ color: 'text.secondary', mb: 3 }}>
          {tab === 0 ? 'Sign in to continue' : 'Create your account'}
        </Typography>

        <Tabs value={tab} onChange={(_, v) => { setTab(v); setError('') }} sx={{ mb: 3 }}>
          <Tab label="Sign in" />
          <Tab label="Register" />
        </Tabs>

        <Box component="form" onSubmit={handleSubmit}>
          <Stack spacing={2}>
            <TextField
              fullWidth
              label="Username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              disabled={loading}
              autoFocus
            />
            <TextField
              fullWidth
              label="Password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              disabled={loading}
            />

            {error && (
              <Typography variant="caption" sx={{ color: 'error.main' }}>
                {error}
              </Typography>
            )}

            <Button
              type="submit"
              variant="contained"
              fullWidth
              disabled={loading || !username.trim() || !password.trim()}
            >
              {loading
                ? 'Please wait...'
                : tab === 0 ? 'Sign in' : 'Create account'}
            </Button>
          </Stack>
        </Box>
      </Paper>
    </Box>
  )
}