// Package kv internal/kv/http.go
package kv

import (
	"encoding/json"
	"net/http"
	"quorum/pkg/logger"
	"time"
)

type HTTPServer struct {
	store *Store
}

func NewHTTPServer(store *Store, addr string) *HTTPServer {
	s := &HTTPServer{store: store}

	mux := http.NewServeMux()
	mux.HandleFunc("/get", s.handleGet)
	mux.HandleFunc("/put", s.handlePut)
	mux.HandleFunc("/delete", s.handleDelete)
	mux.HandleFunc("/health", s.handleHealth)

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("HTTP server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil {
			logger.Error("HTTP server error", "err", err)
		}
	}()

	return s
}

type Response struct {
	Ok    bool   `json:"ok"`
	Value string `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

func (s *HTTPServer) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		s.jsonResponse(w, http.StatusBadRequest, Response{Error: "missing key"})
		return
	}

	value, ok := s.store.Get(key)
	if !ok {
		s.jsonResponse(w, http.StatusNotFound, Response{Error: "key not found"})
		return
	}

	s.jsonResponse(w, http.StatusOK, Response{Ok: true, Value: value})
}

func (s *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonResponse(w, http.StatusMethodNotAllowed, Response{Error: "use POST"})
		return
	}

	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.jsonResponse(w, http.StatusBadRequest, Response{Error: "invalid JSON"})
		return
	}

	if req.Key == "" {
		s.jsonResponse(w, http.StatusBadRequest, Response{Error: "missing key"})
		return
	}

	err := s.store.Put(req.Key, req.Value)
	if err == ErrNotLeader {
		s.jsonResponse(w, http.StatusServiceUnavailable, Response{Error: "not leader"})
		return
	}
	if err != nil {
		s.jsonResponse(w, http.StatusInternalServerError, Response{Error: err.Error()})
		return
	}

	s.jsonResponse(w, http.StatusOK, Response{Ok: true})
}

func (s *HTTPServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.jsonResponse(w, http.StatusMethodNotAllowed, Response{Error: "use POST"})
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		s.jsonResponse(w, http.StatusBadRequest, Response{Error: "missing key"})
		return
	}

	err := s.store.Delete(key)
	if err == ErrNotLeader {
		s.jsonResponse(w, http.StatusServiceUnavailable, Response{Error: "not leader"})
		return
	}
	if err != nil {
		s.jsonResponse(w, http.StatusInternalServerError, Response{Error: err.Error()})
		return
	}

	s.jsonResponse(w, http.StatusOK, Response{Ok: true})
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.jsonResponse(w, http.StatusOK, Response{Ok: true})
}

func (s *HTTPServer) jsonResponse(w http.ResponseWriter, status int, resp Response) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		return
	}
}
