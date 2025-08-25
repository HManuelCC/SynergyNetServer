package dashboard

import "net/http"

func NewApiDashboard() {
	mux := http.NewServeMux()

	http.ListenAndServe(":8090", mux)
}
