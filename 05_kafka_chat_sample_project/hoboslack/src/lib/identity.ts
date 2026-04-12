import { getStoredAuth, getAuthToken } from "./auth";

export function getUserId(): string {
    if(typeof window == "undefined") return "user-anon"
    return getStoredAuth()?.user_id ?? "user-anon"
}

export function getUsername(): string {
    if (typeof window === "undefined") return "anon"
    return getStoredAuth()?.username ?? "anon"
}

export { getAuthToken }

