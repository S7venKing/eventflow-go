package application

import (
	"github.com/google/uuid"
)

func generateID() uuid.UUID {
	return uuid.New()	
}
