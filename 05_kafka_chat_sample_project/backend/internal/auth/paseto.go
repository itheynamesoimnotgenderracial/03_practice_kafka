package auth

import (
	"errors"
	"os"
	"time"

	"aidanwoods.dev/go-paseto"
)

var secretKey paseto.V4SymmetricKey

func init() {
	secret := os.Getenv("PASETO_SECRET_KEY")
	if secret == "" {
		panic("PASETO_SECRET_KEY is required")
	}

	keyBytes := make([]byte, 32)
	copy(keyBytes, []byte(secret))

	var err error
	secretKey, err = paseto.V4SymmetricKeyFromBytes(keyBytes)
	if err != nil {
		panic("invalid PASETO secret key: " + err.Error())
	}
}

func GenerateToken(userID, username string) (string, error) {
	token := paseto.NewToken()
	token.SetString("user_id", userID)
	token.SetString("username", username)
	token.SetIssuedAt(time.Now())
	token.SetExpiration(time.Now().Add(24 * time.Hour))
	token.SetNotBefore(time.Now())
	return token.V4Encrypt(secretKey, nil), nil
}

func ValidateToken(tokenStr string) (userID string, username string, err error) {
	parser := paseto.NewParser()
	parser.AddRule(paseto.NotExpired())
	parser.AddRule(paseto.ValidAt(time.Now()))

	token, err := parser.ParseV4Local(secretKey, tokenStr, nil)
	if err != nil {
		return "", "", errors.New("invalid or expired token")
	}

	userID, err = token.GetString("user_id")
	if err != nil {
		return "", "", errors.New("missing user_id claim")
	}

	username, _ = token.GetString("username")
	return userID, username, nil
}
