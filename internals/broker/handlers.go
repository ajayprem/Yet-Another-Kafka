package broker

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

func (h *Handlers) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

func (h *Handlers) SetLeaderHandler(w http.ResponseWriter, r *http.Request) {
	h.service.setLeader()
	w.WriteHeader(200)
}

func (h *Handlers) SyncHandler(w http.ResponseWriter, r *http.Request) {
	var SyncRequest types.SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&SyncRequest); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	res, err := h.service.syncMessages(SyncRequest.FollowerAddress, SyncRequest.TopicOffsetMap)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (h *Handlers) CreateTopicHandler(w http.ResponseWriter, r *http.Request) {
	var topic types.CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&topic); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.service.createTopic(topic.TopicName, topic.Partitions); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handlers) ProduceHandler(w http.ResponseWriter, r *http.Request) {
	var produceMesssage types.ProduceMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&produceMesssage); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.service.produceMessage(produceMesssage.TopicName, types.Message{Key: produceMesssage.Key, Value: produceMesssage.Value}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(200)
}

func (h *Handlers) FollowHandler(w http.ResponseWriter, r *http.Request) {
	var followMessage types.FollowMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&followMessage); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	lastOffset, err := h.service.apply(followMessage.TopicName, followMessage.Partitions, types.Message{Key: followMessage.Key, Value: followMessage.Value, Offset: followMessage.Offset})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(types.FollowMessageResponse{LastOffset: lastOffset})
}

func (h *Handlers) ConsumeHandler(w http.ResponseWriter, r *http.Request) {
	var registerConsumer types.RegisterConsumerRequest
	if err := json.NewDecoder(r.Body).Decode(&registerConsumer); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.service.registerConsumer(registerConsumer.TopicName, registerConsumer.Address, registerConsumer.FromBeginning); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(200)
}
