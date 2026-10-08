// Copyright 2026 berkaroad
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// The tool's whole value is the classification, so pin all four outcomes: a
// checker that only ever reports "healthy" is worse than none.
func TestCompareClassifiesCopiesAgainstTheLeader(t *testing.T) {
	serve := func(d describe) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasSuffix(r.URL.Path, "/describe") {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"slot":` + strconv.Itoa(int(d.Slot)) +
				`,"node":"` + d.Node + `","loaded":` + strconv.FormatBool(d.Loaded) +
				`,"last_seq":` + strconv.Itoa(int(d.LastSeq)) +
				`,"aggregates":` + strconv.Itoa(int(d.Aggregates)) +
				`,"versions":` + strconv.Itoa(int(d.Versions)) +
				`,"resolvable":` + strconv.Itoa(int(d.Resolvable)) + `}`))
		}))
		t.Cleanup(srv.Close)
		return strings.TrimPrefix(srv.URL, "http://")
	}

	leader := describe{Slot: 7, Node: "node-1", Loaded: true, LastSeq: 100, Aggregates: 1, Versions: 100, Resolvable: 100}

	cases := []struct {
		name     string
		replica  describe
		wantKind string // "" = no finding
	}{
		{
			name:    "in sync",
			replica: describe{Slot: 7, Node: "node-2", Loaded: true, LastSeq: 100, Aggregates: 1, Versions: 100, Resolvable: 100},
		},
		{
			name:     "a lagging copy is reported as behind, not as damage",
			replica:  describe{Slot: 7, Node: "node-2", Loaded: true, LastSeq: 90, Aggregates: 1, Versions: 90, Resolvable: 90},
			wantKind: "behind",
		},
		{
			name:     "the leader's LEO with a short directory is diverged",
			replica:  describe{Slot: 7, Node: "node-2", Loaded: true, LastSeq: 100, Aggregates: 1, Versions: 60, Resolvable: 60},
			wantKind: "diverged",
		},
		{
			name:     "ahead of the leader's LEO with a different directory is diverged too",
			replica:  describe{Slot: 7, Node: "node-2", Loaded: true, LastSeq: 120, Aggregates: 2, Versions: 120, Resolvable: 120},
			wantKind: "diverged",
		},
		{
			name:     "a copy this node never opened is not a mismatch",
			replica:  describe{Slot: 7, Node: "node-2"},
			wantKind: "not-loaded",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adminOf := map[string]string{"node-1": serve(leader), "node-2": serve(tc.replica)}
			got := compare(http.DefaultClient, adminOf, 7, placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}})
			if tc.wantKind == "" {
				if len(got) != 0 {
					t.Fatalf("want no finding, got %+v", got)
				}
				return
			}
			if len(got) != 1 || got[0].Kind != tc.wantKind {
				t.Fatalf("want one %q finding, got %+v", tc.wantKind, got)
			}
			if got[0].Node != "node-2" || got[0].Slot != 7 {
				t.Fatalf("finding names the wrong holder: %+v", got[0])
			}
		})
	}
}

func TestCompareReportsUnreachableHolder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"slot":3,"node":"node-1","loaded":true,"last_seq":50,"aggregates":1,"versions":50,"resolvable":50}`))
	}))
	defer srv.Close()
	adminOf := map[string]string{
		"node-1": strings.TrimPrefix(srv.URL, "http://"),
		"node-2": "127.0.0.1:1", // nothing listens here
	}
	got := compare(http.DefaultClient, adminOf, 3, placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}})
	if len(got) != 1 || got[0].Kind != "unreachable" {
		t.Fatalf("want one unreachable finding, got %+v", got)
	}
}

// A leader this node cannot read leaves nothing to compare against: reporting
// every replica as diverged would be noise, not a finding.
func TestCompareWithoutALeagueViewSaysNothing(t *testing.T) {
	adminOf := map[string]string{"node-2": "127.0.0.1:1"}
	if got := compare(http.DefaultClient, adminOf, 3, placement{Leader: "node-1", Replicas: []string{"node-1", "node-2"}}); got != nil {
		t.Fatalf("want no findings, got %+v", got)
	}
}
