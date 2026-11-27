package vibe

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  64 << 10,
	WriteBufferSize: 64 << 10,
}

// ServeHTTP routes /api/vibe/.
//
//	GET  /api/vibe/status
//	POST /api/vibe/start
//	POST /api/vibe/restart
//	POST /api/vibe/rpc {"method","params","timeout_ms"?}
//	GET  /api/vibe/requests
//	POST /api/vibe/requests/{key}/reply {"result"} | {"error":{"code","message"}}
//	GET  /api/vibe/ws?since=N
//	POST /api/vibe/auth/apikey {"api_key"}
//	GET  /api/vibe/cli/job; POST /api/vibe/cli/{install|update}
func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/vibe")
	switch {
	case path == "/ws":
		m.serveWS(w, r)
	case path == "/rpc" && r.Method == http.MethodPost:
		m.handleRPC(w, r)
	case path == "/requests" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "requests": m.PendingRequests()})
	case strings.HasPrefix(path, "/requests/") && strings.HasSuffix(path, "/reply") && r.Method == http.MethodPost:
		m.handleReply(w, r, strings.TrimSuffix(strings.TrimPrefix(path, "/requests/"), "/reply"))
	case path == "/status" && r.Method == http.MethodGet:
		m.handleStatus(w, r)
	case path == "/start" && r.Method == http.MethodPost:
		if _, err := m.Ensure(r.Context()); err != nil {
			writeJSON(w, http.StatusOK, map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
	case path == "/restart" && r.Method == http.MethodPost:
		m.Stop()
		if _, err := m.Ensure(r.Context()); err != nil {
			writeJSON(w, http.StatusOK, map[string]interface{}{"success": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
	case path == "/auth/apikey" && r.Method == http.MethodPost:
		m.handleAPIKey(w, r)
	case path == "/cli/job" || strings.HasPrefix(path, "/cli/"):
		m.handleInstall(w, r, strings.TrimPrefix(path, "/cli/"))
	default:
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "unknown vibe endpoint"})
	}
}

func (m *Manager) handleStatus(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"success":        true,
		"termux_package": m.termuxNative(),
		"auth_configured": m.hasAuthConfig(),
	}
	bin, err := m.ResolveBinary()
	if err != nil {
		resp["installed"] = false
		resp["error"] = err.Error()
		writeJSON(w, http.StatusOK, resp)
		return
	}
	resp["installed"] = true
	resp["binary"] = bin
	resp["version"] = m.Version(bin)
	m.mu.Lock()
	running := m.proc != nil && m.proc.alive() && m.starting == nil
	resp["starting"] = m.starting != nil
	if m.startErr != nil && !running && !errors.Is(m.startErr, errNotInstalled) {
		resp["error"] = m.startErr.Error()
	}
	if running {
		resp["uptime_seconds"] = int(time.Since(m.proc.started).Seconds())
		if len(m.initInfo) > 0 {
			resp["server"] = m.initInfo
		}
	}
	m.mu.Unlock()
	resp["running"] = running
	m.evMu.Lock()
	resp["pending_requests"] = len(m.pending)
	m.evMu.Unlock()
	resp["logs"] = m.recentLogs(20)
	writeJSON(w, http.StatusOK, resp)
}

func (m *Manager) hasAuthConfig() bool {
	if os.Getenv("MISTRAL_API_KEY") != "" {
		return true
	}
	envPath := filepath.Join(m.HomeDir, ".vibe", ".env")
	if b, err := os.ReadFile(envPath); err == nil && strings.Contains(string(b), "MISTRAL_API_KEY=") {
		return true
	}
	return false
}

func (m *Manager) handleAPIKey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		APIKey string `json:"api_key"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "invalid json"})
		return
	}
	key := strings.TrimSpace(body.APIKey)
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "api_key is required"})
		return
	}
	dir := filepath.Join(m.HomeDir, ".vibe")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	envPath := filepath.Join(dir, ".env")
	content := "MISTRAL_API_KEY=" + key + "\n"
	if err := os.WriteFile(envPath, []byte(content), 0o600); err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

func (m *Manager) handleRPC(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Method    string          `json:"method"`
		Params    json.RawMessage `json:"params"`
		TimeoutMs int             `json:"timeout_ms"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20)).Decode(&body); err != nil || body.Method == "" {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "expected {\"method\", \"params\"}"})
		return
	}
	if bridgeOwnedMethods[body.Method] {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": body.Method + " is handled by the bridge"})
		return
	}
	timeout := 2 * time.Minute
	if body.TimeoutMs > 0 {
		timeout = time.Duration(body.TimeoutMs) * time.Millisecond
		if timeout > 10*time.Minute {
			timeout = 10 * time.Minute
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	res, err := m.Call(ctx, body.Method, body.Params)
	if err != nil {
		resp := map[string]interface{}{"success": false, "error": err.Error()}
		var rpcErr *RPCError
		if errors.As(err, &rpcErr) {
			resp["error"] = rpcErr.Message
			resp["code"] = rpcErr.Code
			if len(rpcErr.Data) > 0 {
				resp["data"] = rpcErr.Data
			}
		} else {
			resp["vibe_unavailable"] = true
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}
	if len(res) == 0 {
		res = json.RawMessage("null")
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "result": res})
}

func (m *Manager) handleReply(w http.ResponseWriter, r *http.Request, key string) {
	var body struct {
		Result json.RawMessage `json:"result"`
		Error  *RPCError       `json:"error"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]interface{}{"success": false, "error": "expected {\"result\"} or {\"error\"}"})
		return
	}
	if err := m.ReplyRequest(key, body.Result, body.Error); err != nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

func (m *Manager) serveWS(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	sub := &subscriber{
		conn: conn,
		send: make(chan []byte, 256),
		done: make(chan struct{}),
	}

	m.evMu.Lock()
	var backlog [][]byte
	for _, ev := range m.events {
		if ev.seq > since {
			backlog = append(backlog, ev.frame)
		}
	}
	m.subs[sub] = struct{}{}
	m.evMu.Unlock()

	defer func() {
		m.evMu.Lock()
		delete(m.subs, sub)
		m.evMu.Unlock()
		_ = conn.Close()
	}()

	for _, frame := range backlog {
		if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
			return
		}
	}

	// Read loop detects client disconnect.
	go func() {
		defer close(sub.done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case frame := <-sub.send:
			if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				return
			}
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return
			}
		case <-sub.done:
			return
		}
	}
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
