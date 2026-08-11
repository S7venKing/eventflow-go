package application

import (
	"github.com/google/uuid"
)

func generateID() string {
	return uuid.NewString()
}
