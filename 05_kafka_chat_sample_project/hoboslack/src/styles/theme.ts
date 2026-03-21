import { createTheme, type ThemeOptions } from '@mui/material/styles'

// ═══════════════════════════════════════════════════════════════════
// GLASSMORPHISM DESIGN TOKENS
// ═══════════════════════════════════════════════════════════════════

export const glass = {
  // Backdrop blur levels
  blur: {
    sm: 'blur(8px)',
    md: 'blur(16px)',
    lg: 'blur(24px)',
    xl: 'blur(40px)',
  },

  // Translucent surface colors
  surface: {
    primary: 'rgba(255, 255, 255, 0.06)',
    elevated: 'rgba(255, 255, 255, 0.10)',
    hover: 'rgba(255, 255, 255, 0.14)',
    active: 'rgba(255, 255, 255, 0.18)',
    overlay: 'rgba(0, 0, 0, 0.4)',
  },

  // Frosted borders
  border: {
    subtle: 'rgba(255, 255, 255, 0.08)',
    default: 'rgba(255, 255, 255, 0.12)',
    strong: 'rgba(255, 255, 255, 0.20)',
    accent: 'rgba(0, 224, 255, 0.30)',
  },

  // Glow / neon effects
  glow: {
    cyan: '0 0 20px rgba(0, 224, 255, 0.15), 0 0 60px rgba(0, 224, 255, 0.05)',
    magenta: '0 0 20px rgba(255, 0, 200, 0.15), 0 0 60px rgba(255, 0, 200, 0.05)',
    violet: '0 0 20px rgba(139, 92, 246, 0.15), 0 0 60px rgba(139, 92, 246, 0.05)',
    warm: '0 0 20px rgba(251, 191, 36, 0.15), 0 0 60px rgba(251, 191, 36, 0.05)',
  },

  // Gradient meshes for backgrounds
  mesh: {
    primary: `
      radial-gradient(ellipse at 20% 50%, rgba(0, 224, 255, 0.08) 0%, transparent 50%),
      radial-gradient(ellipse at 80% 20%, rgba(139, 92, 246, 0.08) 0%, transparent 50%),
      radial-gradient(ellipse at 60% 80%, rgba(255, 0, 200, 0.06) 0%, transparent 50%)
    `,
    card: `
      radial-gradient(ellipse at 0% 0%, rgba(0, 224, 255, 0.04) 0%, transparent 60%),
      radial-gradient(ellipse at 100% 100%, rgba(139, 92, 246, 0.04) 0%, transparent 60%)
    `,
  },
} as const

// ═══════════════════════════════════════════════════════════════════
// PALETTE
// ═══════════════════════════════════════════════════════════════════

const palette = {
  mode: 'dark' as const,
  primary: {
    main: '#00E0FF',
    light: '#66EEFF',
    dark: '#009DBB',
    contrastText: '#000000',
  },
  secondary: {
    main: '#FF00C8',
    light: '#FF66DD',
    dark: '#B3008C',
    contrastText: '#000000',
  },
  error: {
    main: '#FF4D6A',
    light: '#FF8095',
    dark: '#CC3D55',
  },
  warning: {
    main: '#FBBF24',
    light: '#FCD34D',
    dark: '#D97706',
  },
  success: {
    main: '#34D399',
    light: '#6EE7B7',
    dark: '#059669',
  },
  info: {
    main: '#8B5CF6',
    light: '#A78BFA',
    dark: '#6D28D9',
  },
  background: {
    default: '#06060C',
    paper: '#0C0C18',
  },
  text: {
    primary: 'rgba(255, 255, 255, 0.92)',
    secondary: 'rgba(255, 255, 255, 0.56)',
    disabled: 'rgba(255, 255, 255, 0.28)',
  },
  divider: glass.border.default,
}

// ═══════════════════════════════════════════════════════════════════
// TYPOGRAPHY (Distinctive font stack)
// ═══════════════════════════════════════════════════════════════════

const typography = {
  fontFamily: '"Outfit", "Satoshi", "Plus Jakarta Sans", sans-serif',
  h1: {
    fontFamily: '"Outfit", sans-serif',
    fontWeight: 700,
    letterSpacing: '-0.03em',
  },
  h2: {
    fontFamily: '"Outfit", sans-serif',
    fontWeight: 700,
    letterSpacing: '-0.02em',
  },
  h3: {
    fontFamily: '"Outfit", sans-serif',
    fontWeight: 600,
    letterSpacing: '-0.015em',
  },
  h4: {
    fontFamily: '"Outfit", sans-serif',
    fontWeight: 600,
    letterSpacing: '-0.01em',
  },
  h5: {
    fontFamily: '"Outfit", sans-serif',
    fontWeight: 600,
  },
  h6: {
    fontFamily: '"Outfit", sans-serif',
    fontWeight: 600,
  },
  subtitle1: {
    fontWeight: 500,
    letterSpacing: '0.01em',
  },
  subtitle2: {
    fontWeight: 500,
    letterSpacing: '0.02em',
    textTransform: 'uppercase' as const,
    fontSize: '0.7rem',
  },
  body1: {
    lineHeight: 1.7,
  },
  body2: {
    lineHeight: 1.6,
    fontSize: '0.875rem',
  },
  button: {
    fontWeight: 600,
    letterSpacing: '0.03em',
    textTransform: 'none' as const,
  },
  overline: {
    fontWeight: 600,
    letterSpacing: '0.12em',
    fontSize: '0.65rem',
  },
}

// ═══════════════════════════════════════════════════════════════════
// COMPONENT OVERRIDES (Glassmorphism)
// ═══════════════════════════════════════════════════════════════════

const components: ThemeOptions['components'] = {
  MuiCssBaseline: {
    styleOverrides: `
      @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
      
      body {
        background: ${palette.background.default};
        background-image: ${glass.mesh.primary};
        background-attachment: fixed;
        min-height: 100vh;
      }

      ::-webkit-scrollbar {
        width: 6px;
      }
      ::-webkit-scrollbar-track {
        background: transparent;
      }
      ::-webkit-scrollbar-thumb {
        background: ${glass.border.strong};
        border-radius: 3px;
      }
    `,
  },

  MuiPaper: {
    defaultProps: {
      elevation: 0,
    },
    styleOverrides: {
      root: {
        backgroundImage: 'none',
        backgroundColor: glass.surface.primary,
        backdropFilter: glass.blur.md,
        WebkitBackdropFilter: glass.blur.md,
        border: `1px solid ${glass.border.default}`,
        borderRadius: 16,
        transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
      },
    },
  },

  MuiCard: {
    defaultProps: {
      elevation: 0,
    },
    styleOverrides: {
      root: {
        backgroundColor: glass.surface.primary,
        backdropFilter: glass.blur.md,
        WebkitBackdropFilter: glass.blur.md,
        border: `1px solid ${glass.border.default}`,
        borderRadius: 20,
        overflow: 'visible',
        transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
        '&:hover': {
          backgroundColor: glass.surface.hover,
          borderColor: glass.border.accent,
          transform: 'translateY(-2px)',
          boxShadow: glass.glow.cyan,
        },
      },
    },
  },

  MuiButton: {
    defaultProps: {
      disableElevation: true,
    },
    styleOverrides: {
      root: {
        borderRadius: 12,
        padding: '10px 24px',
        fontSize: '0.875rem',
        transition: 'all 0.25s cubic-bezier(0.4, 0, 0.2, 1)',
      },
      contained: {
        background: `linear-gradient(135deg, ${palette.primary.main} 0%, ${palette.info.main} 100%)`,
        color: '#000',
        fontWeight: 700,
        '&:hover': {
          background: `linear-gradient(135deg, ${palette.primary.light} 0%, ${palette.info.light} 100%)`,
          boxShadow: glass.glow.cyan,
          transform: 'translateY(-1px)',
        },
        '&.Mui-disabled': {
          background: glass.surface.primary,
          color: palette.text.disabled,
        },
      },
      outlined: {
        borderColor: glass.border.strong,
        backgroundColor: glass.surface.primary,
        backdropFilter: glass.blur.sm,
        '&:hover': {
          borderColor: palette.primary.main,
          backgroundColor: glass.surface.hover,
          boxShadow: glass.glow.cyan,
        },
      },
      text: {
        '&:hover': {
          backgroundColor: glass.surface.hover,
        },
      },
    },
  },

  MuiTextField: {
    defaultProps: {
      variant: 'outlined',
      size: 'small',
    },
    styleOverrides: {
      root: {
        '& .MuiOutlinedInput-root': {
          borderRadius: 12,
          backgroundColor: glass.surface.primary,
          backdropFilter: glass.blur.sm,
          '& fieldset': {
            borderColor: glass.border.default,
            transition: 'border-color 0.25s ease',
          },
          '&:hover fieldset': {
            borderColor: glass.border.strong,
          },
          '&.Mui-focused fieldset': {
            borderColor: palette.primary.main,
            borderWidth: 1,
            boxShadow: glass.glow.cyan,
          },
        },
      },
    },
  },

  MuiChip: {
    styleOverrides: {
      root: {
        borderRadius: 8,
        fontWeight: 600,
        fontSize: '0.75rem',
        backgroundColor: glass.surface.elevated,
        backdropFilter: glass.blur.sm,
        border: `1px solid ${glass.border.subtle}`,
      },
      colorPrimary: {
        backgroundColor: 'rgba(0, 224, 255, 0.12)',
        borderColor: 'rgba(0, 224, 255, 0.25)',
        color: palette.primary.light,
      },
      colorSecondary: {
        backgroundColor: 'rgba(255, 0, 200, 0.12)',
        borderColor: 'rgba(255, 0, 200, 0.25)',
        color: palette.secondary.light,
      },
    },
  },

  MuiTab: {
    styleOverrides: {
      root: {
        textTransform: 'none',
        fontWeight: 600,
        fontSize: '0.8rem',
        minHeight: 40,
        borderRadius: 10,
        transition: 'all 0.25s ease',
        '&.Mui-selected': {
          backgroundColor: glass.surface.elevated,
          color: palette.primary.main,
        },
      },
    },
  },

  MuiTabs: {
    styleOverrides: {
      root: {
        minHeight: 40,
        backgroundColor: glass.surface.primary,
        borderRadius: 12,
        padding: 4,
        border: `1px solid ${glass.border.subtle}`,
      },
      indicator: {
        display: 'none',
      },
    },
  },

  MuiTooltip: {
    styleOverrides: {
      tooltip: {
        backgroundColor: glass.surface.elevated,
        backdropFilter: glass.blur.md,
        border: `1px solid ${glass.border.default}`,
        borderRadius: 8,
        fontSize: '0.75rem',
        fontWeight: 500,
      },
    },
  },

  MuiDivider: {
    styleOverrides: {
      root: {
        borderColor: glass.border.subtle,
      },
    },
  },

  MuiListItemButton: {
    styleOverrides: {
      root: {
        borderRadius: 12,
        margin: '2px 0',
        transition: 'all 0.2s ease',
        '&:hover': {
          backgroundColor: glass.surface.hover,
        },
        '&.Mui-selected': {
          backgroundColor: glass.surface.elevated,
          borderLeft: `2px solid ${palette.primary.main}`,
          '&:hover': {
            backgroundColor: glass.surface.hover,
          },
        },
      },
    },
  },
}

// ═══════════════════════════════════════════════════════════════════
// THEME EXPORT
// ═══════════════════════════════════════════════════════════════════

const themeOptions: ThemeOptions = {
  palette,
  typography,
  shape: {
    borderRadius: 12,
  },
  components,
}

export const theme = createTheme(themeOptions)

// Re-export for easy access in components
export type AppTheme = typeof theme