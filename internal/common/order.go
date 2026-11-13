// Package common holds the message payload shared by producers and consumers.
package common

import (
	"encoding/json"
	"time"
)

// OrderEvent is the single payload type used across all demos.
// Real systems often use protobuf/Avro + Schema Registry (see docs/06-next-steps.md);
// JSON keeps this starter readable.
type OrderEvent struct {
	ID        string    `json:"id"`         // also used as the Kafka message KEY
	Product   string    `json:"product"`
	Quantity  int       `json:"quantity"`
	Price     float64   `json:"price"`
	CreatedAt time.Time `json:"created_at"` // event time, not Kafka receive time
}

func (e OrderEvent) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

func UnmarshalOrder(b []byte) (OrderEvent, error) {
	var e OrderEvent
	err := json.Unmarshal(b, &e)
	return e, err
}

// MustOrder builds an OrderEvent with a timestamp attached (event time).
func MustOrder(id, product string, qty int, price float64) OrderEvent {
	return OrderEvent{ID: id, Product: product, Quantity: qty, Price: price, CreatedAt: time.Now().UTC()}
}
