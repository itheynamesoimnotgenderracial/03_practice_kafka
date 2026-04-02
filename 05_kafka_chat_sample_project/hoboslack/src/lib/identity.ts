
const USER_ID_KEY = "chat_user_id";

function generateUserId(): string {
    return "user-" + Math.random().toString(36).slice(2,9)
}

export function getUserId(): string {
    if(typeof window == undefined) return "server"

    let id = localStorage.getItem(USER_ID_KEY)
    if(!id) {
        id = generateUserId()
        localStorage.setItem(USER_ID_KEY, id)
    }

    return id
}