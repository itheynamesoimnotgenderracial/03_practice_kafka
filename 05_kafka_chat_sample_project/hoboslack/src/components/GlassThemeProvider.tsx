import { theme } from '#/styles/theme'
import CssBaseline from '@mui/material/CssBaseline'
import { ThemeProvider } from '@mui/material/styles'
import React from 'react'

interface GlassThemeProviderProps {
    children: React.ReactNode
}

const GlassThemeProvider = ({children}: GlassThemeProviderProps) => {
    return <ThemeProvider theme={theme}>
        <CssBaseline />
        {children}
    </ThemeProvider>
}

export default GlassThemeProvider