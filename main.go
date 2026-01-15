package main

import (
	"log"
	"net/http"

	_ "github.com/dioptra-io/retina-orchestrator/docs"

	httpSwagger "github.com/swaggo/http-swagger"
)

// @title Simple API
// @version 1.0
// @description Minimal net/http API with swaggo
// @host localhost:8080
// @BasePath /
func main() {
	http.HandleFunc("/ping", ping)

	http.Handle("/swagger/", httpSwagger.WrapHandler)

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

type PingResponse struct {
	Message string `json:"message"`
}

// ping godoc
// @Summary Ping endpoint
// @Produce json
// @Success 200 {object} PingResponse
// @Router /ping [get]
func ping(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"message":"pong"}`))
}
