/*
Package frontend implements handlers for front-end, and Register function for adding them to mux.Router.
*/
package frontend

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	server "github.com/shanduur/simple-srv"
	"github.com/shanduur/squat/server/api"
	"github.com/shanduur/squat/server/website"
)

// RegisterEndpoints registers all handlers for the application.
func RegisterEndpoints(mux *mux.Router) {
	mux.HandleFunc("/", handleIndex).Methods(http.MethodGet)
	mux.HandleFunc("/table", handleTable).Methods(http.MethodGet)
}

func handleIndex(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(website.BuildIndex(api.Providers)))
}

func handleTable(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	src := req.FormValue("source")
	tab := req.FormValue("table")

	p := api.Providers[src]
	if p != nil {
		dsc, err := p.GetTableDescription(tab)
		if err != nil {
			server.PrintError(w, fmt.Errorf("unable to get table description: %s", err.Error()), http.StatusNotFound)
			return
		}

		out, err := website.BuildTables(src, tab, dsc)
		if err != nil {
			server.PrintError(w, fmt.Errorf("unable to build tables: %s", err.Error()), http.StatusInternalServerError)
			return
		}
		w.Write([]byte(out))
	}
}
