package event

import (
	"errors"
	"strings"
	"testing"
)

func TestParseGHArchive(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		want    Event
		wantErr bool
	}{
		{
			name: "push event",
			line: `{"id":"1","type":"PushEvent","actor":{"login":"octocat"},"repo":{"name":"golang/go"},"payload":{"size":1},"created_at":"2024-01-01T15:04:05Z"}`,
			want: Event{TS: 1704121445, Type: "PushEvent", Repo: "golang/go", Actor: "octocat"},
		},
		{
			name: "missing actor is allowed",
			line: `{"type":"WatchEvent","repo":{"name":"a/b"},"created_at":"2024-01-01T00:00:00Z"}`,
			want: Event{TS: 1704067200, Type: "WatchEvent", Repo: "a/b"},
		},
		{name: "missing type", line: `{"repo":{"name":"a/b"},"created_at":"2024-01-01T00:00:00Z"}`, wantErr: true},
		{name: "missing repo", line: `{"type":"PushEvent","created_at":"2024-01-01T00:00:00Z"}`, wantErr: true},
		{name: "missing created_at", line: `{"type":"PushEvent","repo":{"name":"a/b"}}`, wantErr: true},
		{name: "bad timestamp", line: `{"type":"PushEvent","repo":{"name":"a/b"},"created_at":"yesterday"}`, wantErr: true},
		{name: "not json", line: `not json`, wantErr: true},
		{name: "empty object", line: `{}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGHArchive([]byte(tt.line))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseMissingFieldIsTyped(t *testing.T) {
	_, err := ParseGHArchive([]byte(`{"type":"PushEvent"}`))
	if !errors.Is(err, ErrMissingField) {
		t.Fatalf("err = %v, want ErrMissingField", err)
	}
}

func TestBinaryRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
	}{
		{"typical", Event{TS: 1704121445, Type: "PushEvent", Repo: "golang/go", Actor: "octocat"}},
		{"empty strings", Event{TS: 1}},
		{"negative ts", Event{TS: -42, Type: "X", Repo: "y/z", Actor: "a"}},
		{"unicode", Event{TS: 7, Type: "IssuesEvent", Repo: "日本/リポ", Actor: "üser"}},
		{"long repo", Event{TS: 9, Type: "ForkEvent", Repo: strings.Repeat("r", 5000), Actor: "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := AppendBinary(nil, tt.ev)
			got, n, err := DecodeBinary(b)
			if err != nil {
				t.Fatal(err)
			}
			if n != len(b) {
				t.Fatalf("consumed %d of %d bytes", n, len(b))
			}
			if got != tt.ev {
				t.Fatalf("got %+v, want %+v", got, tt.ev)
			}
		})
	}
}

func TestDecodeConsecutive(t *testing.T) {
	a := Event{TS: 1, Type: "A", Repo: "r/1", Actor: "x"}
	b := Event{TS: 2, Type: "B", Repo: "r/2", Actor: "y"}
	buf := AppendBinary(AppendBinary(nil, a), b)
	got1, n, err := DecodeBinary(buf)
	if err != nil || got1 != a {
		t.Fatalf("first: %+v %v", got1, err)
	}
	got2, _, err := DecodeBinary(buf[n:])
	if err != nil || got2 != b {
		t.Fatalf("second: %+v %v", got2, err)
	}
}

func TestDecodeBinaryTruncated(t *testing.T) {
	b := AppendBinary(nil, Event{TS: 1704121445, Type: "PushEvent", Repo: "golang/go", Actor: "octocat"})
	for i := 0; i < len(b); i++ {
		if _, _, err := DecodeBinary(b[:i]); err == nil {
			t.Fatalf("prefix of %d/%d bytes decoded without error", i, len(b))
		}
	}
}

func TestField(t *testing.T) {
	e := Event{Type: "T", Repo: "R", Actor: "A"}
	tests := []struct {
		col  string
		want string
		ok   bool
	}{
		{"type", "T", true}, {"repo", "R", true}, {"actor", "A", true}, {"ts", "", false}, {"", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.col, func(t *testing.T) {
			got, ok := e.Field(tt.col)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("Field(%q) = %q,%v want %q,%v", tt.col, got, ok, tt.want, tt.ok)
			}
			if ValidColumn(tt.col) != tt.ok {
				t.Fatalf("ValidColumn(%q) = %v", tt.col, !tt.ok)
			}
		})
	}
}
