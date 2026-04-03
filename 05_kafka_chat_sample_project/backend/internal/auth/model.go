package auth

type User struct {
	ID           string `bson:"_id" json:"id"`
	Username     string `bson:"username" json:"username"`
	PasswordHash string `bson:"password_hash" json:"-"`
	CreatedAt    int64  `bson:"created_at" json:"created_at"`
}
