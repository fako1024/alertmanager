package main

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/alertmanager/types"
)

type ReceivedAlerts struct {
	Receiver        string        `json:"receiver"`
	Status          string        `json:"status"`
	Alerts          []types.Alert `json:"alerts"`
	ExternalURL     string        `json:"externalURL"`
	Version         string        `json:"version"`
	GroupKey        string        `json:"groupKey"`
	TruncatedAlerts int           `json:"truncatedAlerts"`
}

type webhookConsumer struct {
	nReceived uint64
}

func newWebhookConsumer() *webhookConsumer {
	return &webhookConsumer{
		nReceived: 0,
	}
}

func (wc *webhookConsumer) ListenAndServe(endpoint string) error {
	// Start a simple HTTP server to handle incoming webhook requests
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var alerts ReceivedAlerts
		if json.NewDecoder(r.Body).Decode(&alerts) != nil {
			http.Error(w, "failed to decode JSON", http.StatusBadRequest)
			return
		}
		atomic.AddUint64(&wc.nReceived, uint64(len(alerts.Alerts)))
		w.WriteHeader(http.StatusOK)
	})

	return http.ListenAndServe(endpoint, nil)
}

func (wc *webhookConsumer) GetNNotifications() uint64 {
	return atomic.LoadUint64(&wc.nReceived)
}
