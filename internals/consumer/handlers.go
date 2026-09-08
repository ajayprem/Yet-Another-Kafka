package consumer

import (
	"encoding/json"
	"net/http"
	"yet-another-kafka/internals/types"
)

type Handlers struct {
	service *Service
}

func NewHandlers(service *Service) *Handlers {
	return &Handlers{service: service}
}

func (h *Handlers) MessageConsumeHandler(w http.ResponseWriter, r *http.Request) {
	var message types.ConsumerMessageData

	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		http.Error(w, "invalid message", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	h.service.consume(
		types.Message{
			Offset: message.Offset,
			Key:    message.Key,
			Value:  message.Value,
		},
	)

	w.WriteHeader(http.StatusNoContent)
}
