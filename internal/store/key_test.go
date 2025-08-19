package store

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestMakePinRecordKey(t *testing.T) {
	key := makePinRecordKey("bafy123")
	if string(key) != "p/bafy123" {
		t.Fatalf("unexpected pin key: %q", key)
	}
}

func TestMakeStatusKeyAndPrefix(t *testing.T) {
	prefix := makeStatusPrefix(StatusActive)
	if string(prefix) != "s/02/" {
		t.Fatalf("unexpected status prefix: %q", prefix)
	}

	const ts int64 = 123456789
	key := makeStatusKey(StatusActive, ts, "bafy")

	if !bytes.HasPrefix(key, prefix) {
		t.Fatalf("status key should have prefix %q, got %q", prefix, key)
	}

	if !bytes.HasSuffix(key, []byte("/bafy")) {
		t.Fatalf("status key should have cid suffix, got %q", key)
	}
}

func TestMakeExpireKeyAndParse(t *testing.T) {
	const ts int64 = 987654321
	key := makeExpireKey(ts, "abc")
	t.Logf("key: %s", key)

	parsedTs, cid, err := parseExpireKey(key)
	if err != nil {
		t.Fatalf("parseExpireKey returned error: %v", err)
	}
	if parsedTs != ts {
		t.Fatalf("ts mismatch: got %d want %d", parsedTs, ts)
	}
	if cid != "abc" {
		t.Fatalf("cid mismatch: got %q want %q", cid, "abc")
	}
}

func TestParseExpireKeyErrors(t *testing.T) {
	// invalid structure (not 3 parts)
	if _, _, err := parseExpireKey([]byte("e/onlytwo")); err == nil || err != ErrInvalidExpireKey {
		t.Fatalf("expected ErrInvalidExpireKey, got %v", err)
	}

	// wrong prefix with valid 3-part structure
	var buf bytes.Buffer
	buf.WriteString("x/")
	// write 8-byte big-endian int
	var b8 [8]byte
	binary.BigEndian.PutUint64(b8[:], uint64(42))
	buf.Write(b8[:])
	buf.WriteString("/cid")

	if _, _, err := parseExpireKey(buf.Bytes()); err == nil || err != ErrWrongExpirePrefix {
		t.Fatalf("expected ErrWrongExpirePrefix, got %v", err)
	}
}
