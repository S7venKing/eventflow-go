package application

import (
	"crypto/rand"
	"time"

	"github.com/oklog/ulid/v2"
)

func generateEventID() string {
	entropy := ulid.Monotonic(
		rand.Reader,
		0,
	)

	id := ulid.MustNew(
		ulid.Timestamp(time.Now()),
		entropy,
	)

	return id.String()
}
