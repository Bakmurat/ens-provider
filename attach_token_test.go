package main

// v0.1.23 (backlog item 13): join-token TTL — deterministic tests per
// the reviewed plan (C-05 pure-builder seam, C-06 bounded parser).

import (
	"testing"
	"time"

	"github.com/alibabacloud-go/tea/tea"
)

func TestParseJoinTokenTTL(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"", 2 * time.Hour, false},        // default: twice Alibaba's documented 1h validity
		{"  ", 2 * time.Hour, false},      // whitespace = unset
		{"0", 0, false},                   // escape hatch: omit Expired, endpoint default applies
		{"1h", time.Hour, false},          // floor inclusive
		{"24h", 24 * time.Hour, false},    // cap inclusive
		{"2h30m", 2*time.Hour + 30*time.Minute, false},
		{"59m", 0, true},                  // below floor: worse than the documented default
		{"2m", 0, true},                   // the typo class the floor exists for
		{"25h", 0, true},                  // above cap: no day-long-plus credentials by typo
		{"200h", 0, true},
		{"-2h", 0, true},
		{"garbage", 0, true},
		{"7200", 0, true},                 // bare number (not a duration, not "0")
	}
	for _, tc := range cases {
		got, err := parseJoinTokenTTL(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseJoinTokenTTL(%q): want error, got %v", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseJoinTokenTTL(%q): unexpected error %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseJoinTokenTTL(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestBuildAttachScriptsRequestExpired(t *testing.T) {
	g := &Group{ID: "np-test", NodepoolID: "np-test"}
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)

	// default policy: exact absolute timestamp now+2h
	req := buildAttachScriptsRequest(&Config{JoinTokenTTL: 2 * time.Hour}, g, now)
	if req.Expired == nil {
		t.Fatal("default TTL: Expired must be set")
	}
	if want := now.Add(2 * time.Hour).Unix(); *req.Expired != want {
		t.Errorf("Expired = %d, want %d (now+2h exactly)", *req.Expired, want)
	}

	// custom TTL
	req = buildAttachScriptsRequest(&Config{JoinTokenTTL: 5 * time.Hour}, g, now)
	if want := now.Add(5 * time.Hour).Unix(); req.Expired == nil || *req.Expired != want {
		t.Errorf("custom TTL: Expired = %v, want %d", req.Expired, want)
	}

	// 0 = omit the field entirely (endpoint default applies)
	req = buildAttachScriptsRequest(&Config{JoinTokenTTL: 0}, g, now)
	if req.Expired != nil {
		t.Errorf("TTL 0: Expired must be nil (omit), got %d", *req.Expired)
	}

	// the rest of the request is unchanged by the feature
	if tea.StringValue(req.NodepoolId) != "np-test" || tea.StringValue(req.Arch) != "amd64" {
		t.Errorf("request basics changed: nodepool=%q arch=%q",
			tea.StringValue(req.NodepoolId), tea.StringValue(req.Arch))
	}
}
