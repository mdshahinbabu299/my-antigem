package vibe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type installJob struct {
	mu       sync.Mutex
	Kind     string
	Running  bool
	ExitCode *int
	Log      strings.Builder
	Started  time.Time
}

func (j *installJob) logf(format string, a ...interface{}) {
	j.mu.Lock()
	defer j.mu.Unlock()
	fmt.Fprintf(&j.Log, format+"\n", a...)
}

func (j *installJob) snapshot() map[string]interface{} {
	j.mu.Lock()
	defer j.mu.Unlock()
	return map[string]interface{}{
		"kind":      j.Kind,
		"running":   j.Running,
		"exit_code": j.ExitCode,
		"log":       j.Log.String(),
		"started":   j.Started,
	}
}

func (m *Manager) handleInstall(w http.ResponseWriter, r *http.Request, sub string) {
	switch {
	case sub == "job" && r.Method == http.MethodGet:
		m.jobMu.Lock()
		j := m.job
		m.jobMu.Unlock()
		if j == nil {
			writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "job": nil})
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "job": j.snapshot()})
	case (sub == "install" || sub == "update") && r.Method == http.MethodPost:
		m.jobMu.Lock()
		if m.job != nil && m.job.Running {
			snap := m.job.snapshot()
			m.jobMu.Unlock()
			writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "already_running": true, "job": snap})
			return
		}
		j := &installJob{Kind: sub, Running: true, Started: time.Now()}
		m.job = j
		m.jobMu.Unlock()
		go m.runInstallJob(j, sub == "update")
		writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "job": j.snapshot()})
	default:
		writeJSON(w, http.StatusNotFound, map[string]interface{}{"success": false, "error": "unknown vibe cli endpoint"})
	}
}

func (m *Manager) runInstallJob(j *installJob, update bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	err := m.install(ctx, j, update)
	code := 0
	if err != nil {
		code = 1
		j.logf("ERROR: %v", err)
	}
	j.mu.Lock()
	j.Running = false
	j.ExitCode = &code
	j.mu.Unlock()
}

func (m *Manager) install(ctx context.Context, j *installJob, update bool) error {
	if uv, err := exec.LookPath("uv"); err == nil {
		args := []string{"tool", "install", "mistral-vibe"}
		if update {
			args = []string{"tool", "upgrade", "mistral-vibe"}
		}
		j.logf("Running: %s %s", uv, strings.Join(args, " "))
		if err := m.runLogged(ctx, j, uv, args...); err == nil {
			return m.verifyAfterInstall(j)
		}
	}
	if pipx, err := exec.LookPath("pipx"); err == nil {
		args := []string{"install", "mistral-vibe"}
		if update {
			args = []string{"upgrade", "mistral-vibe"}
		}
		j.logf("Running: %s %s", pipx, strings.Join(args, " "))
		if err := m.runLogged(ctx, j, pipx, args...); err == nil {
			return m.verifyAfterInstall(j)
		}
	}
	for _, py := range []string{"python3", "python"} {
		if bin, err := exec.LookPath(py); err == nil {
			args := []string{"-m", "pip", "install", "--user"}
			if update {
				args = append(args, "--upgrade")
			}
			args = append(args, "mistral-vibe")
			j.logf("Running: %s %s", bin, strings.Join(args, " "))
			if err := m.runLogged(ctx, j, bin, args...); err == nil {
				return m.verifyAfterInstall(j)
			}
		}
	}
	return errors.New("could not install mistral-vibe: install uv, pipx, or python3-pip first")
}

func (m *Manager) verifyAfterInstall(j *installJob) error {
	bin, err := m.ResolveBinary()
	if err != nil {
		return err
	}
	j.logf("Installed vibe-acp at %s (version: %s)", bin, m.Version(bin))
	return nil
}

func (m *Manager) runLogged(ctx context.Context, j *installJob, bin string, args ...string) error {
	cmd := m.command(ctx, bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return err
	}
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 32<<10), 1<<20)
	for sc.Scan() {
		j.logf("%s", sc.Text())
	}
	return cmd.Wait()
}
