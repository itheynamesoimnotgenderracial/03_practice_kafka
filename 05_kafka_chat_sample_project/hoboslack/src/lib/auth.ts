import { createServerFn } from "@tanstack/react-start";
import z from "zod"
import { API_BASE_URL } from "./constants";

const TOKEN_KEY = "chat_token"
const USER_KEY = "chat_user"

export interface AuthUser {
    user_id: string;
    username: string;
    token: string;
}

export function getStoredAuth(): AuthUser | null {
    if (typeof window === "undefined") return null

    try {
        const raw = localStorage.getItem(USER_KEY)
        return raw ? (JSON.parse(raw) as AuthUser) : null
    } catch(error) {
        console.log("getStoredAuth error ===>", error)
        return null
    }
}

export function storeAuth(user: AuthUser) {
    localStorage.setItem(USER_KEY, JSON.stringify(user))
    localStorage.setItem(TOKEN_KEY, user.token)
}

export function clearAuth() {
    localStorage.removeItem(USER_KEY)
    localStorage.removeItem(TOKEN_KEY)
}

export function getAuthToken(): string {
    if (typeof window === "undefined") return ""
    return localStorage.getItem(TOKEN_KEY) ?? ""
}

// ── Server functions ─────────────────────────────────────────────────
const loginSchema = z.object({
    username: z.string().min(1),
    password: z.string().min(1)
})

const registerSchema = z.object({
    username: z.string().min(3).max(32),
    password: z.string().min(6),
})

export const loginRequest = createServerFn({method: "POST"})
    .inputValidator(loginSchema)
    .handler(async ({ data }) => {
        const res = await fetch(`${API_BASE_URL}api/auth/login`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(data),
        })

        if (!res.ok) {
            await res.json().catch((error) => console.log(console.log("login failederror: ", error)))
            return
        }

        return res.json() as Promise<AuthUser>
    })

export const registerRequest = createServerFn({ method: "POST" })
    .inputValidator(registerSchema)
    .handler(async ({ data }) => {
        const res = await fetch(`${API_BASE_URL}api/auth/register`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(data),
        })

        if (!res.ok) {
            await res.json().catch((error) => console.log(console.log("Registration failed error: ", error)))
            return
        }

        return res.json() as Promise<AuthUser>
    })