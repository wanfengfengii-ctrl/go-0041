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

func TestEmptyDepartmentCannotTakeCustodyOrDispose(t *testing.T) {
	newHandler := func(t *testing.T) (*httpapi.Server, *testutil.System) {
		t.Helper()
		s := testutil.NewSystem(t)
		keys := infra.NewMapKeyStore(map[string][]byte{"k1": []byte("secret")})
		return httpapi.New(s.Coord, s.Store, lims.NewAdapter(s.Coord, keys), s.IDGen), s
	}
	postHandler := func(t *testing.T, srv *httpapi.Server, path string, body any) *http.Response {
		t.Helper()
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		srv.ServeHTTP(recorder, req)
		return recorder.Result()
	}
	returnMother := func(t *testing.T, s *testutil.System, familyID, entityID string) {
		t.Helper()
		testutil.Register(s, t, familyID, entityID, "donor", "lab", 100)
		if _, err := s.Coord.Submit(domain.Command{
			Op: domain.OpReturn, Principal: testutil.Op("lab"),
			FamilyID: familyID, EntityID: entityID,
		}); err != nil {
			t.Fatalf("return %s: %v", entityID, err)
		}
	}
	assertUnchanged := func(t *testing.T, s *testutil.System, familyID, entityID string, revision, seq uint64, committed int64) {
		t.Helper()
		fam := s.Coord.Family(familyID)
		if fam.Revision != revision || s.Coord.AppliedSeq() != seq || s.Store.CommittedAt() != committed {
			t.Fatalf("denied command changed revision, sequence, or log offset: rev=%d seq=%d offset=%d", fam.Revision, s.Coord.AppliedSeq(), s.Store.CommittedAt())
		}
		if fam.Mother.ID != entityID || fam.Mother.CustodyDept != "" || fam.Mother.Status != domain.StatusActive {
			t.Fatalf("denied command changed entity: %+v", fam.Mother)
		}
		if len(fam.Seals) != 0 || len(fam.Destructions) != 0 {
			t.Fatalf("denied command created audit records: seals=%d destructions=%d", len(fam.Seals), len(fam.Destructions))
		}
	}
	assertPermission := func(t *testing.T, err error, op string) {
		t.Helper()
		se := scerr.As(err)
		if se == nil || se.Code != scerr.CodePermissionDenied || se.Field != "department" || se.Operation != op {
			t.Fatalf("expected structured department permission error for %s, got %v", op, err)
		}
	}

	t.Run("coordinator claim", func(t *testing.T) {
		s := testutil.NewSystem(t)
		returnMother(t, s, "fam-claim", "m-claim")
		revision, seq, committed := s.Coord.Family("fam-claim").Revision, s.Coord.AppliedSeq(), s.Store.CommittedAt()
		_, err := s.Coord.Submit(domain.Command{
			Op: domain.OpClaim, Principal: domain.Principal{Operator: "empty", Role: domain.RoleOperator},
			FamilyID: "fam-claim", EntityID: "m-claim",
		})
		assertPermission(t, err, domain.OpClaim)
		assertUnchanged(t, s, "fam-claim", "m-claim", revision, seq, committed)
	})

	t.Run("HTTP seal and destroy", func(t *testing.T) {
		srv, s := newHandler(t)
		returnMother(t, s, "fam-http", "m-http")
		revision, seq, committed := s.Coord.Family("fam-http").Revision, s.Coord.AppliedSeq(), s.Store.CommittedAt()
		for _, cmd := range []domain.Command{
			{Op: domain.OpSeal, FamilyID: "fam-http", EntityID: "m-http", SealID: "seal-empty", Principal: domain.Principal{Operator: "empty", Role: domain.RoleApprover}},
			{Op: domain.OpDestroy, FamilyID: "fam-http", EntityID: "m-http", DestructionID: "destroy-empty", Principal: domain.Principal{Operator: "empty", Role: domain.RoleApprover}},
		} {
			resp := postHandler(t, srv, "/commands", cmd)
			var se scerr.Error
			if err := json.NewDecoder(resp.Body).Decode(&se); err != nil {
				t.Fatalf("decode %s error: %v", cmd.Op, err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("%s status = %d, want %d", cmd.Op, resp.StatusCode, http.StatusForbidden)
			}
			assertPermission(t, &se, cmd.Op)
			assertUnchanged(t, s, "fam-http", "m-http", revision, seq, committed)
		}
	})

	t.Run("signed LIMS batch is atomic", func(t *testing.T) {
		s := testutil.NewSystem(t)
		returnMother(t, s, "fam-valid", "m-valid")
		returnMother(t, s, "fam-empty", "m-empty")
		adapter := lims.NewAdapter(s.Coord, infra.NewMapKeyStore(map[string][]byte{"k1": []byte("secret")}))
		records := []lims.Record{
			{Op: domain.OpClaim, FamilyID: "fam-valid", EntityID: "m-valid", Operator: "valid", Department: "new-lab", Role: "operator"},
			{Op: domain.OpDestroy, FamilyID: "fam-empty", EntityID: "m-empty", DestructionID: "destroy-empty", Operator: "empty", Role: "approver"},
		}
		sig, err := lims.Sign([]byte("secret"), records)
		if err != nil {
			t.Fatalf("sign batch: %v", err)
		}
		body, err := lims.EncodeJSON(records, "k1", sig)
		if err != nil {
			t.Fatalf("encode batch: %v", err)
		}
		seq, committed := s.Coord.AppliedSeq(), s.Store.CommittedAt()
		_, err = adapter.Import("json", body)
		se := scerr.As(err)
		if se == nil || se.Code != scerr.CodeBatchPartialInvalid || len(se.Details) != 1 || se.Details[0].RecordIndex != 1 || se.Details[0].Code != scerr.CodePermissionDenied {
			t.Fatalf("expected record 1 permission rejection, got %v", err)
		}
		assertUnchanged(t, s, "fam-valid", "m-valid", 2, seq, committed)
		assertUnchanged(t, s, "fam-empty", "m-empty", 2, seq, committed)
	})

	t.Run("non-empty custodian remains authorized", func(t *testing.T) {
		srv, s := newHandler(t)
		returnMother(t, s, "fam-authorized", "m-authorized")
		if _, err := s.Coord.Submit(domain.Command{
			Op: domain.OpClaim, Principal: testutil.Op("new-lab"),
			FamilyID: "fam-authorized", EntityID: "m-authorized",
		}); err != nil {
			t.Fatalf("claim: %v", err)
		}
		resp := postHandler(t, srv, "/commands", domain.Command{
			Op: domain.OpSeal, Principal: testutil.Approver("new-lab"),
			FamilyID: "fam-authorized", EntityID: "m-authorized", SealID: "seal-valid",
		})
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("seal status = %d, want %d", resp.StatusCode, http.StatusOK)
		}
		adapter := lims.NewAdapter(s.Coord, infra.NewMapKeyStore(map[string][]byte{"k1": []byte("secret")}))
		records := []lims.Record{{
			Op: domain.OpDestroy, FamilyID: "fam-authorized", EntityID: "m-authorized", DestructionID: "destroy-valid",
			Operator: "approver", Department: "new-lab", Role: "approver",
		}}
		sig, _ := lims.Sign([]byte("secret"), records)
		body, _ := lims.EncodeJSON(records, "k1", sig)
		if _, err := adapter.Import("json", body); err != nil {
			t.Fatalf("destroy batch: %v", err)
		}
		fam := s.Coord.Family("fam-authorized")
		if fam.Revision != 5 || fam.Mother.Status != domain.StatusDestroyed || len(fam.Seals) != 1 || len(fam.Destructions) != 1 {
			t.Fatalf("unexpected authorized final state: %+v", fam)
		}
	})
}

// silence unused import warnings for strings if not otherwise used
var _ = strings.TrimSpace
