// Package httpapi exposes the specimen-custody-graph service over HTTP.
// Endpoints:
//
//	POST /commands                 submit a single command
//	POST /batches                  import a LIMS batch (json or csv)
//	GET  /families                 list family ids
//	GET  /families/{id}            family detail (canonical state)
//	GET  /families/{id}/lineage    directed pedigree
//	GET  /families/{id}/reconcile  volume reconciliation report
//	GET  /families/{id}/events     event audit trail
//	GET  /healthz                  health check
//
// Errors are rendered as the structured error contract with a stable HTTP
// status mapping.
package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"specimen-custody-graph/coord"
	"specimen-custody-graph/domain"
	"specimen-custody-graph/eventstore"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/invariant"
	"specimen-custody-graph/lims"
	"specimen-custody-graph/scerr"
)

// Server holds the HTTP dependencies.
type Server struct {
	Coord   *coord.Coordinator
	Store   *eventstore.Store
	Adapter *lims.Adapter
	IDGen   infra.IDGen
	mux     *http.ServeMux
}

// New constructs a Server and registers routes.
func New(c *coord.Coordinator, store *eventstore.Store, adapter *lims.Adapter, idgen infra.IDGen) *Server {
	s := &Server{Coord: c, Store: store, Adapter: adapter, IDGen: idgen, mux: http.NewServeMux()}
	s.routes()
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("POST /commands", s.handleCommand)
	s.mux.HandleFunc("POST /batches", s.handleBatch)
	s.mux.HandleFunc("GET /families", s.handleListFamilies)
	s.mux.HandleFunc("GET /families/{id}", s.handleFamily)
	s.mux.HandleFunc("GET /families/{id}/lineage", s.handleLineage)
	s.mux.HandleFunc("GET /families/{id}/reconcile", s.handleReconcile)
	s.mux.HandleFunc("GET /families/{id}/events", s.handleEvents)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ok",
		"applied_seq": s.Coord.AppliedSeq(),
	})
}

// commandRequest is the body of POST /commands. Missing ids are filled from
// the id generator so callers can omit them.
type commandRequest struct {
	domain.Command
	AutoFamily bool `json:"auto_family,omitempty"`
}

func (s *Server) handleCommand(w http.ResponseWriter, r *http.Request) {
	var req commandRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, err)
		return
	}
	cmd := req.Command
	// fill generated ids where the caller omitted them
	if cmd.Op == domain.OpSeal && cmd.SealID == "" {
		cmd.SealID = s.IDGen.NewSealID()
	}
	if cmd.Op == domain.OpDestroy && cmd.DestructionID == "" {
		cmd.DestructionID = s.IDGen.NewDestructionID()
	}
	if cmd.Op == domain.OpLoadStation && cmd.StationID == "" {
		cmd.StationID = s.IDGen.NewStationID()
	}
	if cmd.Op == domain.OpRegister && cmd.EntityID == "" {
		cmd.EntityID = "mother-" + s.IDGen.NewFamilyID()
	}
	ev, err := s.Coord.Submit(cmd)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"event":    ev,
		"revision": ev.Revision,
		"sequence": ev.Seq,
	})
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	ct := r.Header.Get("Content-Type")
	// accept "application/json", "text/csv", or a simple "json"/"csv"
	switch {
	case strings.Contains(ct, "csv"):
		ct = "csv"
	case strings.Contains(ct, "json"):
		ct = "json"
	case ct == "":
		ct = "json"
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, scerr.New(scerr.CodeParseError, "read body: "+err.Error()))
		return
	}
	events, err := s.Adapter.Import(ct, data)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"imported": len(events),
		"events":   events,
	})
}

func (s *Server) handleListFamilies(w http.ResponseWriter, r *http.Request) {
	fams := s.Coord.Families()
	ids := make([]string, 0, len(fams))
	for id := range fams {
		ids = append(ids, id)
	}
	writeJSON(w, http.StatusOK, map[string]any{"families": ids, "count": len(ids)})
}

func (s *Server) handleFamily(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fam := s.Coord.Family(id)
	if fam == nil {
		writeError(w, scerr.New(scerr.CodeNotFound, "family not found: "+id).WithEntity(id))
		return
	}
	writeJSON(w, http.StatusOK, fam.Canonical())
}

func (s *Server) handleLineage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fam := s.Coord.Family(id)
	if fam == nil {
		writeError(w, scerr.New(scerr.CodeNotFound, "family not found: "+id).WithEntity(id))
		return
	}
	writeJSON(w, http.StatusOK, fam.Lineage())
}

func (s *Server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	fam := s.Coord.Family(id)
	if fam == nil {
		writeError(w, scerr.New(scerr.CodeNotFound, "family not found: "+id).WithEntity(id))
		return
	}
	rep := invariant.Check(fam)
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var events []domain.Event
	err := s.Store.Replay(func(ev domain.Event) error {
		if ev.FamilyID == id {
			events = append(events, ev)
		}
		return nil
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"family_id": id, "events": events, "count": len(events)})
}

// decodeBody decodes the JSON request body into v.
func decodeBody(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return scerr.New(scerr.CodeParseError, "invalid request body: "+err.Error())
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	se := scerr.As(err)
	if se == nil {
		se = scerr.New(scerr.CodeStorage, err.Error())
	}
	writeJSON(w, scerr.HTTPStatus(se.Code), se)
}

// ParseInt is a small helper for query parameters.
func ParseInt(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
