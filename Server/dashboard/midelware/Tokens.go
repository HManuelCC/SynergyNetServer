package midelware

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Token struct {
	Token        string `json:"token"`
	RefreshToken string `json:"refresh_token"`
}

var secretPrivateKey = []byte("H@des$2024")

func GenerateToken() Token {
	// Create the JWT claims
	claims := jwt.MapClaims{
		"authorized": true,
		"client":     "myapp",
		"exp":        jwt.NewNumericDate(time.Now().Add(time.Hour * 72)), // 72 hours expiration
	}

	// Generate the token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(secretPrivateKey)
	if err != nil {
		// Handle error
	}

	return Token{
		Token:        tokenString,
		RefreshToken: "myrefreshsecrettoken",
	}
}
