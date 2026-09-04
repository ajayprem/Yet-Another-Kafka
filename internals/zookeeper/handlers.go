package zookeeper

import (
	types "yet-another-kafka/internals/types"
	"encoding/json"
	"errors"
	"net/http"
)

type Handlers struct {
	reg *Registry
}

func NewHandlers(reg *Registry) *Handlers {
	return &Handlers{
		reg: reg,
	}
}

func (h *Handlers) RegisterBroker(w http.ResponseWriter, r *http.Request) {
	var body Broker
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	isLeader, err := h.reg.RegisterBroker(&body)
	if err != nil {
		if errors.Is(err, ErrBrokerIDConflict) {
			http.Error(w, err.Error(), http.StatusConflict) // 409
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return

	}

	w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(types.RegisterBrokerResponse{IsLeader: isLeader})
}

func (h *Handlers) GetLeader(w http.ResponseWriter, r *http.Request) {
	leader, ok := h.reg.GetLeader()
	if !ok {
		http.Error(w, "no connected brokers", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(types.BrokerAddressResponse{Address: leader.Address})
}

func (h *Handlers) GetRandomBroker(w http.ResponseWriter, r *http.Request) {
	broker, ok := h.reg.GetRandomBroker()
	if !ok {
		http.Error(w, "no connected brokers", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(types.BrokerAddressResponse{Address: broker.Address})
}
