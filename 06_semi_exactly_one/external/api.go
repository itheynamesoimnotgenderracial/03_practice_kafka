package external

import (
	"fmt"
	"log"
	"math/rand"
	"simple-semo-eos/internal/models"
)

type CallerFunc func(msg models.CustomerMessage) (*models.ExternalAPIResponse, error)

// CallCustomerAPI simulates a 3rd-party verification API with 30% failure rate.
func CallCustomerAPI(msg models.CustomerMessage) (*models.ExternalAPIResponse, error) {
	log.Printf("[ExtAPI] -> calling | customer_id=%s", msg.CustomerID)
	if rand.Float32() < 0.3 {
		return nil, fmt.Errorf("simulated third-party timeout")
	}

	return &models.ExternalAPIResponse{
		ExternalID: fmt.Sprintf("EXT-%s-%d", msg.CustomerID, rand.Intn(9999)),
	}, nil
}
