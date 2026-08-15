package httpapi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"specimen-custody-graph/domain"
	"specimen-custody-graph/httpapi"
	"specimen-custody-graph/infra"
	"specimen-custody-graph/lims"
	"specimen-custody-graph/scerr"
	"specimen-custody-graph/testutil"
)

func newServer(t *testing.T) (*httptest.Server, *testutil.System) {
	t.Helper()
	s := testutil.NewSystem(t)
	keys := infra.NewMapKeyStore(map[string][]byte{"k1": []byte("secret")})
	adapter := lims.NewAdapter(s.Coord, keys)
	srv := httpapi.New(s.Coord, s.Store, adapter, s.IDGen)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, s
}

func post(t *testing.T, ts *httptest.Server, path string, body any) *http.Response {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(ts.URL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func TestHealthEndpoint(t *testing.T) {
	ts, _ := newServer(t)
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestCommandSubmitAndQuery(t *testing.T) {
	ts, s := newServer(t)
	_ = s
	// register
	resp := post(t, ts, "/commands", map[string]any{
		"op": "register", "family_id": "fam-1", "entity_id": "m1", "source": "s", "volume": 1000,
		"principal": map[string]any{"operator": "o", "department": "lab", "role": "operator"},
	})
	if resp.StatusCode != 200 {
		t.Fatalf("register status = %d", resp.StatusCode)
	}
	// lineage query
	resp2, err := http.Get(ts.URL + "/families/fam-1/lineage")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp2.StatusCode != 200 {
		t.Fatalf("lineage status = %d", resp2.StatusCode)
	}
	// reconcile
	resp3, err := http.Get(ts.URL + "/families/fam-1/reconcile")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp3.StatusCode != 200 {
		t.Fatalf("reconcile status = %d", resp3.StatusCode)
	}
}

func TestReconcileBalanced(t *testing.T) {
	ts, _ := newServer(t)
	post(t, ts, "/commands", map[string]any{
		"op": "register", "family_id": "fam-1", "entity_id": "m1", "source": "s", "volume": 1000,
		"principal": map[string]any{"operator": "o", "department": "lab", "role": "operator"},
	})
	post(t, ts, "/commands", map[string]any{
		"op": "aliquot", "family_id": "fam-1", "parent_id": "m1", "child_id": "t1", "volume": 200,
		"principal": map[string]any{"operator": "o", "department": "lab", "role": "operator"},
	})
	resp, err := http.Get(ts.URL + "/families/fam-1/reconcile")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	var rep struct {
		Balanced bool `json:"balanced"`
		Total    int  `json:"total"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&rep)
	if !rep.Balanced {
		t.Fatal("expected balanced")
	}
	if rep.Total != 1000 {
		t.Fatalf("total = %d, want 1000", rep.Total)
	}
}

func TestErrorStatusMapping(t *testing.T) {
	ts, _ := newServer(t)
	// unknown family -> 404
	resp, err := http.Get(ts.URL + "/families/nope")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.StatusCode != 404 {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	// malformed command -> 400
	resp, err = http.Post(ts.URL+"/commands", "application/json", strings.NewReader("{bad"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestBatchImportEndpoint(t *testing.T) {
	ts, _ := newServer(t)
	records := []lims.Record{
		{Op: domain.OpRegister, FamilyID: "fam-b", EntityID: "m1", Source: "s", Volume: 1000, Operator: "o", Department: "lab", Role: "operator"},
	}
	sig, _ := lims.Sign([]byte("secret"), records)
	jb, _ := lims.EncodeJSON(records, "k1", sig)
	resp, err := http.Post(ts.URL+"/batches", "application/json", bytes.NewReader(jb))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestPermissionDeniedStatus(t *testing.T) {
	ts, _ := newServer(t)
	// auditor attempts a write -> 403
	resp := post(t, ts, "/commands", map[string]any{
		"op": "register", "family_id": "fam-1", "entity_id": "m1", "source": "s", "volume": 100,
		"principal": map[string]any{"operator": "o", "department": "lab", "role": "auditor"},
	})
	if resp.StatusCode != 403 {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	var se scerr.Error
	_ = json.NewDecoder(resp.Body).Decode(&se)
	if se.Code != scerr.CodePermissionDenied {
		t.Fatalf("code = %s", se.Code)
	}
}

func TestEventsAuditEndpoint(t *testing.T) {
	ts, _ := newServer(t)
	post(t, ts, "/commands", map[string]any{
		"op": "register", "family_id": "fam-1", "entity_id": "m1", "source": "s", "volume": 1000,
		"principal": map[string]any{"operator": "o", "department": "lab", "role": "operator"},
	})
	resp, err := http.Get(ts.URL + "/families/fam-1/events")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out struct {
		Count int `json:"count"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.Count != 1 {
		t.Fatalf("count = %d, want 1", out.Count)
	}
}

// silence unused import warnings for strings if not otherwise used
var _ = strings.TrimSpace
