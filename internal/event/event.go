// Package event defines the GitHub event row stored by ghstore and its
// parsing (from GH Archive JSON) and binary encoding (for the WAL).
package event

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Columns lists the string columns that can be grouped or filtered on.
var Columns = []string{"type", "repo", "actor"}

// Event is one flattened GitHub event. TS is Unix seconds (UTC).
type Event struct {
	TS    int64
	Type  string
	Repo  string
	Actor string
}

// Field returns the value of a string column by name.
func (e Event) Field(col string) (string, bool) {
	switch col {
	case "type":
		return e.Type, true
	case "repo":
		return e.Repo, true
	case "actor":
		return e.Actor, true
	}
	return "", false
}

// ValidColumn reports whether col is a known string column.
func ValidColumn(col string) bool {
	_, ok := Event{}.Field(col)
	return ok
}

// ghArchiveEvent mirrors the subset of a GH Archive record we keep.
type ghArchiveEvent struct {
	Type      string `json:"type"`
	CreatedAt string `json:"created_at"`
	Actor     struct {
		Login string `json:"login"`
	} `json:"actor"`
	Repo struct {
		Name string `json:"name"`
	} `json:"repo"`
}

// ErrMissingField is returned when a record lacks a required field.
var ErrMissingField = errors.New("event: missing required field")

// ParseGHArchive parses one line of a GH Archive hourly dump.
func ParseGHArchive(line []byte) (Event, error) {
	var raw ghArchiveEvent
	if err := json.Unmarshal(line, &raw); err != nil {
		return Event{}, fmt.Errorf("event: bad json: %w", err)
	}
	if raw.Type == "" || raw.CreatedAt == "" || raw.Repo.Name == "" {
		return Event{}, ErrMissingField
	}
	t, err := time.Parse(time.RFC3339, raw.CreatedAt)
	if err != nil {
		return Event{}, fmt.Errorf("event: bad created_at %q: %w", raw.CreatedAt, err)
	}
	return Event{TS: t.Unix(), Type: raw.Type, Repo: raw.Repo.Name, Actor: raw.Actor.Login}, nil
}

// AppendBinary appends the compact binary form of e to buf:
// varint ts, then type, repo, actor as uvarint-length-prefixed strings.
func AppendBinary(buf []byte, e Event) []byte {
	buf = binary.AppendVarint(buf, e.TS)
	for _, s := range [...]string{e.Type, e.Repo, e.Actor} {
		buf = binary.AppendUvarint(buf, uint64(len(s)))
		buf = append(buf, s...)
	}
	return buf
}

// ErrCorrupt is returned when binary data cannot be decoded.
var ErrCorrupt = errors.New("event: corrupt encoding")

// DecodeBinary decodes one event produced by AppendBinary and returns the
// number of bytes consumed.
func DecodeBinary(b []byte) (Event, int, error) {
	var e Event
	ts, n := binary.Varint(b)
	if n <= 0 {
		return e, 0, ErrCorrupt
	}
	e.TS = ts
	off := n
	var strs [3]string
	for i := range strs {
		l, n := binary.Uvarint(b[off:])
		if n <= 0 || uint64(len(b)-off-n) < l {
			return Event{}, 0, ErrCorrupt
		}
		off += n
		strs[i] = string(b[off : off+int(l)])
		off += int(l)
	}
	e.Type, e.Repo, e.Actor = strs[0], strs[1], strs[2]
	return e, off, nil
}
