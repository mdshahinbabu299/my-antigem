package vibe

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDisabledStatusWhenMissing(t *testing.T) {
	tmp := t.TempDir()
	mgr := NewManager(tmp, tmp, filepath.Join(tmp, "missing-vibe-acp"))
	req := httptest.NewRequest(http.MethodGet, "/api/vibe/status", nil)
	rec := httptest.NewRecorder()
	mgr.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp["installed"] != false {
		t.Fatalf("expected installed=false, got %v", resp["installed"])
	}
}

func TestAPIKeyEndpointWritesEnv(t *testing.T) {
	tmp := t.TempDir()
	mgr := NewManager(tmp, tmp, "")
	body := bytes.NewBufferString(`{"api_key":"test-secret-key"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/vibe/auth/apikey", body)
	rec := httptest.NewRecorder()
	mgr.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	data, err := os.ReadFile(filepath.Join(tmp, ".vibe", ".env"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "MISTRAL_API_KEY=test-secret-key\n" {
		t.Fatalf("unexpected .env content: %q", string(data))
	}
}

func TestMockACPRoundTripAndPermissionReply(t *testing.T) {
	tmp := t.TempDir()
	script := filepath.Join(tmp, "mock-vibe-acp")
	src := `#!/usr/bin/env python3
import json, sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    rid = msg.get("id")
    if method == "initialize":
        print(json.dumps({"jsonrpc": "2.0", "id": rid, "result": {"protocolVersion": 1, "agentInfo": {"name": "vibe-acp", "version": "0.1.0"}}}), flush=True)
    elif method == "session/new":
        print(json.dumps({"jsonrpc": "2.0", "id": rid, "result": {"sessionId": "sess-1"}}), flush=True)
    elif method == "session/prompt":
        print(json.dumps({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": "sess-1", "update": {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "hello"}}}}), flush=True)
        print(json.dumps({"jsonrpc": "2.0", "id": 99, "method": "session/request_permission", "params": {"sessionId": "sess-1", "toolCall": {"title": "run ls"}}}), flush=True)
        reply_line = sys.stdin.readline()
        reply = json.loads(reply_line)
        print(json.dumps({"jsonrpc": "2.0", "id": rid, "result": {"stopReason": "end_turn", "reply": reply.get("result")}}), flush=True)
`
	if err := os.WriteFile(script, []byte(src), 0o755); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(tmp, tmp, script)
	defer mgr.StopAll()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := mgr.Call(ctx, "session/new", json.RawMessage(`{"cwd":"."}`))
	if err != nil {
		t.Fatalf("session/new failed: %v", err)
	}
	if !bytes.Contains(res, []byte("sess-1")) {
		t.Fatalf("unexpected session/new result: %s", string(res))
	}

	promptDone := make(chan json.RawMessage, 1)
	promptErr := make(chan error, 1)
	go func() {
		r, e := mgr.Call(ctx, "session/prompt", json.RawMessage(`{"sessionId":"sess-1","prompt":[{"type":"text","text":"hi"}]}`))
		if e != nil {
			promptErr <- e
			return
		}
		promptDone <- r
	}()

	var key string
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		reqs := mgr.PendingRequests()
		if len(reqs) == 1 {
			key = reqs[0].Key
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if key == "" {
		t.Fatal("timed out waiting for pending permission request")
	}
	if err := mgr.ReplyRequest(key, json.RawMessage(`{"outcome":{"outcome":"selected","optionId":"allow_once"}}`), nil); err != nil {
		t.Fatalf("ReplyRequest failed: %v", err)
	}

	select {
	case err := <-promptErr:
		t.Fatalf("session/prompt failed: %v", err)
	case r := <-promptDone:
		if !bytes.Contains(r, []byte("end_turn")) {
			t.Fatalf("unexpected session/prompt result: %s", string(r))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session/prompt")
	}
}
