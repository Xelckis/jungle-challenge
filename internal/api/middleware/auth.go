package middleware

import (
	"cmp"
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/gin-gonic/gin"
)

type AuthMiddleware struct {
	verifier *oidc.IDTokenVerifier
}

func NewAuthMiddleware() (*AuthMiddleware, error) {
	providerURL := cmp.Or(os.Getenv("KEYCLOAK_URL"), "http://127.0.0.1:8080/realms/jungle-realm")
	ctx := context.Background()

	var provider *oidc.Provider
	var err error

	maxRetries := 15
	for i := range maxRetries {
		provider, err = oidc.NewProvider(ctx, providerURL)
		if err == nil {
			break
		}
		log.Printf("Aguardando Keycloak inicializar o realm (tentativa %d/%d)...", i+1, maxRetries)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		return nil, err
	}

	verifier := provider.Verifier(&oidc.Config{
		SkipClientIDCheck: true,
	})

	return &AuthMiddleware{verifier: verifier}, nil
}

func (m *AuthMiddleware) RequireProviderId() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token ausente"})
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "formato de token invalido"})
			return
		}

		idToken, err := m.verifier.Verify(c.Request.Context(), parts[1])
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "token invalido ou expirado"})
			return
		}

		var claims struct {
			ClientID string `json:"clientId"` // Usado pelo Keycloak no client_credentials
			Azp      string `json:"azp"`      // Authorized party
		}
		if err := idToken.Claims(&claims); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "falha ao extrair claims"})
			return
		}

		providerId := claims.ClientID
		if providerId == "" {
			providerId = claims.Azp
		}

		if providerId == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "providerId nao identificado no token"})
			return
		}

		c.Set("providerId", providerId)
		c.Next()
	}
}
