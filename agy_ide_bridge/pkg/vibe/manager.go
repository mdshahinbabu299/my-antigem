// Package vibe runs Mistral Vibe CLI (https://github.com/mistralai/mistral-vibe) for the app via `vibe-acp`
// (Agent Client Protocol, JSON-RPC 2.0 over stdin/stdout).
package vibe

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	startTimeout      = 45 * time.Second
	maxBufferedEvents = 20000
	maxBufferedBytes  = 48 << 20
)

var clientVersion = "1.0.0"

var appHandledRequests = map[string]bool{
	"session/request_permission": true,
	"fs/read_text_file":          true,
	"fs/write_text_file":         true,
}

var bridgeOwnedMethods = map[string]bool{
	"initialize":  true,
	"initialized": true,
}

var errNotInstalled = errors.New("vibe-acp not found")

// RPCError is a JSON-RPC 2.0 error returned by vibe-acp.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("vibe: %s (code %d)", e.Message, e.Code) }

type rpcResult struct {
	result json.RawMessage
	err    error
}

type wireMessage struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// PendingRequest is a server->client request (e.g. session/request_permission) waiting for an answer.
type PendingRequest struct {
	Key       string          `json:"key"`
	Method    string          `json:"method"`
	Params    json.RawMessage `json:"params"`
	SessionID string          `json:"session_id,omitempty"`
	Seq       int64           `json:"seq"`
	Received  time.Time       `json:"received"`
	rawID     json.RawMessage
	gen       int64
}

type event struct {
	seq   int64
	frame []byte
}

type subscriber struct {
	conn *websocket.Conn
	send chan []byte
	done chan struct{}
}

type process struct {
	gen     int64
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	writeMu sync.Mutex
	exited  chan struct{}

	mu      sync.Mutex
	nextID  int64
	calls   map[int64]chan rpcResult
	started time.Time
}

func (p *process) alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

func (m *Manager) liveProc() *process {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.proc != nil && m.proc.alive() && m.starting == nil {
		return m.proc
	}
	return nil
}

// Manager owns the vibe-acp process and serves /api/vibe/.
type Manager struct {
	HomeDir      string
	WorkspaceDir string
	BinPath      string

	mu       sync.Mutex
	proc     *process
	gen      int64
	starting chan struct{}
	startErr error
	initInfo json.RawMessage
	logs     []string

	evMu       sync.Mutex
	events     []event
	evBytes    int
	nextSeq    int64
	subs       map[*subscriber]struct{}
	pending    map[string]*PendingRequest
	pendingIDs map[string]string
	reqCounter int64

	verMu      sync.Mutex
	verBin     string
	verModTime time.Time
	version    string

	jobMu sync.Mutex
	job   *installJob
}

// NewManager creates a Manager for Mistral Vibe ACP.
func NewManager(homeDir, workspaceDir, binPath string) *Manager {
	binPath = strings.TrimSpace(binPath)
	if binPath == "~" || strings.HasPrefix(binPath, "~/") {
		binPath = filepath.Join(homeDir, strings.TrimPrefix(binPath, "~"))
	}
	return &Manager{
		HomeDir:      homeDir,
		WorkspaceDir: workspaceDir,
		BinPath:      binPath,
		subs:         make(map[*subscriber]struct{}),
		pending:      make(map[string]*PendingRequest),
		pendingIDs:   make(map[string]string),
	}
}

func (m *Manager) termuxPrefix() string {
	return os.Getenv("PREFIX")
}

func (m *Manager) termuxNative() bool {
	return strings.Contains(m.termuxPrefix(), "/com.termux/") || strings.Contains(m.termuxPrefix(), "/ai.antigem") || strings.Contains(m.termuxPrefix(), "/ai.antidev")
}

// ResolveBinary finds the vibe-acp executable.
func (m *Manager) ResolveBinary() (string, error) {
	if m.BinPath != "" {
		if fi, err := os.Stat(m.BinPath); err == nil && !fi.IsDir() {
			return m.BinPath, nil
		}
		return "", fmt.Errorf("%w: %s", errNotInstalled, m.BinPath)
	}
	var candidates []string
	candidates = append(candidates, "vibe-acp")
	if prefix := m.termuxPrefix(); prefix != "" {
		candidates = append(candidates, filepath.Join(prefix, "bin", "vibe-acp"))
	}
	candidates = append(candidates,
		filepath.Join(m.HomeDir, ".local", "bin", "vibe-acp"),
		filepath.Join(m.HomeDir, ".local", "share", "uv", "tools", "mistral-vibe", "bin", "vibe-acp"),
		"/usr/local/bin/vibe-acp",
		"/usr/bin/vibe-acp",
	)
	for _, c := range candidates {
		if !strings.Contains(c, "/") {
			if p, err := exec.LookPath(c); err == nil {
				return p, nil
			}
			continue
		}
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return c, nil
		}
	}
	return "", fmt.Errorf("%w (install mistral-vibe from the app or pass --vibe-bin)", errNotInstalled)
}

// ResolveCLI finds the `vibe` CLI binary (used for --version or setup).
func (m *Manager) ResolveCLI() string {
	if p, err := exec.LookPath("vibe"); err == nil {
		return p
	}
	if prefix := m.termuxPrefix(); prefix != "" {
		c := filepath.Join(prefix, "bin", "vibe")
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			return c
		}
	}
	c := filepath.Join(m.HomeDir, ".local", "bin", "vibe")
	if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
		return c
	}
	return ""
}

func (m *Manager) command(ctx context.Context, bin string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, bin, args...)
	env := os.Environ()
	hasTmp := false
	for _, e := range env {
		if strings.HasPrefix(e, "TMPDIR=") && len(e) > len("TMPDIR=") {
			hasTmp = true
		}
	}
	if prefix := m.termuxPrefix(); !hasTmp && prefix != "" {
		env = append(env, "TMPDIR="+filepath.Join(prefix, "tmp"))
	}
	cmd.Env = env
	return cmd
}

// Version returns the installed mistral-vibe version if available.
func (m *Manager) Version(bin string) string {
	fi, err := os.Stat(bin)
	if err != nil {
		return ""
	}
	m.verMu.Lock()
	defer m.verMu.Unlock()
	if m.verBin == bin && m.verModTime.Equal(fi.ModTime()) && m.version != "" {
		return m.version
	}
	cli := m.ResolveCLI()
	if cli == "" {
		cli = bin
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := m.command(ctx, cli, "--version").Output()
	if err != nil {
		return ""
	}
	v := strings.TrimSpace(string(out))
	m.verBin, m.verModTime, m.version = bin, fi.ModTime(), v
	return m.version
}

func (m *Manager) addLog(line string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, line)
	if len(m.logs) > 200 {
		m.logs = m.logs[len(m.logs)-200:]
	}
}

func (m *Manager) recentLogs(n int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.logs) <= n {
		return append([]string(nil), m.logs...)
	}
	return append([]string(nil), m.logs[len(m.logs)-n:]...)
}

// Ensure starts vibe-acp if it is not already running.
func (m *Manager) Ensure(ctx context.Context) (*process, error) {
	for {
		m.mu.Lock()
		if m.starting != nil {
			ch := m.starting
			m.mu.Unlock()
			select {
			case <-ch:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			m.mu.Lock()
			p := m.proc
			err := m.startErr
			m.mu.Unlock()
			if p != nil && p.alive() {
				return p, nil
			}
			if err != nil {
				return nil, err
			}
			continue
		}
		if m.proc != nil && m.proc.alive() {
			p := m.proc
			m.mu.Unlock()
			return p, nil
		}
		done := make(chan struct{})
		m.starting = done
		m.mu.Unlock()

		p, err := m.start()

		m.mu.Lock()
		m.startErr = err
		m.starting = nil
		m.mu.Unlock()
		close(done)
		if err != nil {
			return nil, err
		}
		m.broadcastState("spawned", "")
		return p, nil
	}
}

func (m *Manager) start() (*process, error) {
	bin, err := m.ResolveBinary()
	if err != nil {
		return nil, err
	}
	cmd := m.command(context.Background(), bin)
	dir := m.WorkspaceDir
	if fi, err := os.Stat(dir); dir == "" || err != nil || !fi.IsDir() {
		dir = m.HomeDir
	}
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting vibe-acp: %w", err)
	}

	m.mu.Lock()
	m.gen++
	p := &process{
		gen:     m.gen,
		cmd:     cmd,
		stdin:   stdin,
		exited:  make(chan struct{}),
		calls:   make(map[int64]chan rpcResult),
		started: time.Now(),
	}
	m.proc = p
	m.mu.Unlock()

	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			m.addLog(sc.Text())
		}
	}()
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		m.readLoop(p, stdout)
	}()
	go func() {
		<-readerDone
		_ = cmd.Wait()
		close(p.exited)
		m.onExit(p)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	info, err := m.call(ctx, p, "initialize", map[string]interface{}{
		"protocolVersion": 1,
		"clientInfo":      map[string]interface{}{"name": "antigem", "title": "AntiGem", "version": clientVersion},
		"clientCapabilities": map[string]interface{}{
			"fs": map[string]interface{}{
				"readTextFile":  false,
				"writeTextFile": false,
			},
		},
	})
	if err != nil {
		m.stopProcess(p)
		if logs := m.recentLogs(6); len(logs) > 0 {
			return nil, fmt.Errorf("vibe-acp did not start: %v (%s)", err, strings.Join(logs, " | "))
		}
		return nil, fmt.Errorf("vibe-acp did not start: %w", err)
	}
	m.mu.Lock()
	m.initInfo = info
	m.mu.Unlock()
	return p, nil
}

func (m *Manager) onExit(p *process) {
	p.mu.Lock()
	calls := p.calls
	p.calls = map[int64]chan rpcResult{}
	p.mu.Unlock()
	for _, ch := range calls {
		ch <- rpcResult{err: errors.New("vibe-acp exited")}
	}
	m.evMu.Lock()
	for k, r := range m.pending {
		if r.gen == p.gen {
			delete(m.pending, k)
		}
	}
	for k := range m.pendingIDs {
		if strings.HasPrefix(k, fmt.Sprintf("%d:", p.gen)) {
			delete(m.pendingIDs, k)
		}
	}
	m.evMu.Unlock()

	errText := ""
	if code := p.cmd.ProcessState; code != nil && !code.Success() {
		errText = code.String()
		if logs := m.recentLogs(4); len(logs) > 0 {
			errText += ": " + strings.Join(logs, " | ")
		}
	}
	m.broadcastState("exited", errText)
}

func (m *Manager) stopProcess(p *process) {
	if p == nil {
		return
	}
	_ = p.stdin.Close()
	if p.cmd.Process != nil {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
	}
	select {
	case <-p.exited:
	case <-time.After(3 * time.Second):
		if p.cmd.Process != nil {
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		}
		<-p.exited
	}
}

// Stop stops the running vibe-acp process.
func (m *Manager) Stop() {
	m.mu.Lock()
	p := m.proc
	m.proc = nil
	m.initInfo = nil
	m.mu.Unlock()
	m.stopProcess(p)
}

// StopAll stops vibe-acp and closes all WebSocket subscribers.
func (m *Manager) StopAll() {
	m.Stop()
	m.evMu.Lock()
	for sub := range m.subs {
		close(sub.done)
		_ = sub.conn.Close()
		delete(m.subs, sub)
	}
	m.evMu.Unlock()
}

func (m *Manager) write(p *process, msg interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_, err = p.stdin.Write(b)
	return err
}

// Call sends a JSON-RPC 2.0 request to vibe-acp, starting it if needed.
func (m *Manager) Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	p, err := m.Ensure(ctx)
	if err != nil {
		return nil, err
	}
	var pVal interface{}
	if len(params) > 0 && string(params) != "null" {
		pVal = params
	}
	return m.call(ctx, p, method, pVal)
}

func (m *Manager) call(ctx context.Context, p *process, method string, params interface{}) (json.RawMessage, error) {
	p.mu.Lock()
	p.nextID++
	id := p.nextID
	ch := make(chan rpcResult, 1)
	p.calls[id] = ch
	p.mu.Unlock()

	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  method,
	}
	if params != nil {
		req["params"] = params
	}
	if err := m.write(p, req); err != nil {
		p.mu.Lock()
		delete(p.calls, id)
		p.mu.Unlock()
		return nil, err
	}

	select {
	case res := <-ch:
		return res.result, res.err
	case <-ctx.Done():
		p.mu.Lock()
		delete(p.calls, id)
		p.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (m *Manager) readLoop(p *process, stdout io.Reader) {
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var msg wireMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			m.addLog("vibe-acp invalid json: " + line)
			continue
		}
		// Response to one of our outgoing requests.
		if len(msg.ID) > 0 && msg.Method == "" {
			var id int64
			if err := json.Unmarshal(msg.ID, &id); err == nil {
				p.mu.Lock()
				ch, ok := p.calls[id]
				if ok {
					delete(p.calls, id)
				}
				p.mu.Unlock()
				if ok {
					if msg.Error != nil {
						ch <- rpcResult{err: msg.Error}
					} else {
						ch <- rpcResult{result: msg.Result}
					}
				}
			}
			continue
		}
		// Server -> client request (e.g. session/request_permission).
		if len(msg.ID) > 0 && msg.Method != "" {
			if !appHandledRequests[msg.Method] {
				_ = m.write(p, map[string]interface{}{
					"jsonrpc": "2.0",
					"id":      msg.ID,
					"error":   map[string]interface{}{"code": -32601, "message": "unsupported client method: " + msg.Method},
				})
				continue
			}
			m.recordPendingRequest(p, msg)
			continue
		}
		// Notification (e.g. session/update).
		if msg.Method != "" {
			m.recordEvent(map[string]interface{}{
				"type":   "notification",
				"method": msg.Method,
				"params": jsonRawOrNull(msg.Params),
			})
		}
	}
}

func jsonRawOrNull(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	return raw
}

func (m *Manager) recordPendingRequest(p *process, msg wireMessage) {
	var parsed struct {
		SessionID string `json:"sessionId"`
	}
	_ = json.Unmarshal(msg.Params, &parsed)

	m.evMu.Lock()
	m.reqCounter++
	key := fmt.Sprintf("r%d", m.reqCounter)
	pr := &PendingRequest{
		Key:       key,
		Method:    msg.Method,
		Params:    jsonRawOrNull(msg.Params),
		SessionID: parsed.SessionID,
		Received:  time.Now(),
		rawID:     append(json.RawMessage(nil), msg.ID...),
		gen:       p.gen,
	}
	m.pending[key] = pr
	m.pendingIDs[fmt.Sprintf("%d:%s", p.gen, string(msg.ID))] = key
	m.evMu.Unlock()

	seq := m.recordEvent(map[string]interface{}{
		"type":       "request",
		"key":        key,
		"method":     msg.Method,
		"session_id": parsed.SessionID,
		"params":     pr.Params,
	})
	m.evMu.Lock()
	if cur, ok := m.pending[key]; ok {
		cur.Seq = seq
	}
	m.evMu.Unlock()
}

// PendingRequests returns all unanswered server->client requests.
func (m *Manager) PendingRequests() []*PendingRequest {
	m.evMu.Lock()
	defer m.evMu.Unlock()
	out := make([]*PendingRequest, 0, len(m.pending))
	for _, r := range m.pending {
		out = append(out, r)
	}
	return out
}

// ReplyRequest sends a JSON-RPC response back to vibe-acp for a pending request.
func (m *Manager) ReplyRequest(key string, result json.RawMessage, rpcErr *RPCError) error {
	m.evMu.Lock()
	pr, ok := m.pending[key]
	if ok {
		delete(m.pending, key)
		delete(m.pendingIDs, fmt.Sprintf("%d:%s", pr.gen, string(pr.rawID)))
	}
	m.evMu.Unlock()
	if !ok {
		return fmt.Errorf("unknown or already answered request: %s", key)
	}
	p := m.liveProc()
	if p == nil || p.gen != pr.gen {
		return errors.New("vibe-acp process restarted before reply")
	}
	resp := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      pr.rawID,
	}
	if rpcErr != nil {
		resp["error"] = rpcErr
	} else {
		resp["result"] = jsonRawOrNull(result)
	}
	if err := m.write(p, resp); err != nil {
		return err
	}
	m.recordEvent(map[string]interface{}{
		"type": "request_resolved",
		"key":  key,
	})
	return nil
}

func (m *Manager) broadcastState(state, errText string) {
	payload := map[string]interface{}{
		"type":  "state",
		"state": state,
	}
	if errText != "" {
		payload["error"] = errText
	}
	m.recordEvent(payload)
}

func (m *Manager) recordEvent(payload map[string]interface{}) int64 {
	m.evMu.Lock()
	defer m.evMu.Unlock()
	m.nextSeq++
	seq := m.nextSeq
	payload["seq"] = seq
	frame, err := json.Marshal(payload)
	if err != nil {
		return seq
	}
	m.events = append(m.events, event{seq: seq, frame: frame})
	m.evBytes += len(frame)
	for (len(m.events) > maxBufferedEvents || m.evBytes > maxBufferedBytes) && len(m.events) > 1 {
		m.evBytes -= len(m.events[0].frame)
		m.events = m.events[1:]
	}
	for sub := range m.subs {
		select {
		case sub.send <- frame:
		default:
			// slow subscriber drops oldest implicitly or closes on overflow
		}
	}
	return seq
}
