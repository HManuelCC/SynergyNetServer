package router

import "net/http"

func NewRouter(mux *http.ServeMux) {
	// Define your routes here
	// Example:
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Server is running"))
	})

}
