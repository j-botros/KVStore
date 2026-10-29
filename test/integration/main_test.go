//go:build integration

package integration

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
)

// binPath is the kvstore binary built once in TestMain and shared by every test.
var binPath string

// tracked holds every child process started by the harness, so TestMain can
// kill anything a test failed to clean up before checking for leaks.
var (
	trackedMu sync.Mutex
	tracked   = map[int]*os.Process{}
)

func track(p *os.Process)   { trackedMu.Lock(); tracked[p.Pid] = p; trackedMu.Unlock() }
func untrack(p *os.Process) { trackedMu.Lock(); delete(tracked, p.Pid); trackedMu.Unlock() }

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(run(m))
}

func run(m *testing.M) int {
	root, err := repoRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration: %v\n", err)
		return 2
	}

	binDir, err := os.MkdirTemp("", "kvstore-it-bin-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "integration: mkdir temp: %v\n", err)
		return 2
	}
	defer os.RemoveAll(binDir)
	binPath = filepath.Join(binDir, "kvstore")

	// Build the binary once. KVSTORE_IT_RACE=1 builds it with the race detector.
	args := []string{"build", "-o", binPath}
	if os.Getenv("KVSTORE_IT_RACE") == "1" {
		args = append(args, "-race")
	}
	args = append(args, "./cmd/kvstore")
	build := exec.Command("go", args...)
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "integration: go build failed: %v\n%s", err, out)
		return 2
	}

	code := m.Run()

	// Safety net: kill anything still tracked, then make sure no process
	// running the built binary survived the run.
	trackedMu.Lock()
	for _, p := range tracked {
		_ = p.Signal(syscall.SIGCONT)
		_ = p.Kill()
	}
	trackedMu.Unlock()

	if pids := leakedPIDs(); len(pids) > 0 {
		fmt.Fprintf(os.Stderr, "integration: LEAKED kvstore processes after the run: %v (killing them)\n", pids)
		for _, pid := range pids {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		if code == 0 {
			code = 1
		}
	} else {
		fmt.Fprintf(os.Stderr, "integration: no leaked kvstore processes (pgrep -f %s)\n", binPath)
	}
	return code
}

// repoRoot returns the module root (the directory holding go.mod), two levels
// above this package.
func repoRoot() (string, error) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("go.mod not found at %s: %w", root, err)
	}
	return root, nil
}

// leakedPIDs lists processes whose command line contains the built binary path.
func leakedPIDs() []int {
	out, err := exec.Command("pgrep", "-f", binPath).Output()
	if err != nil {
		return nil // pgrep exits 1 when nothing matches
	}
	var pids []int
	for _, f := range strings.Fields(string(bytes.TrimSpace(out))) {
		if pid, err := strconv.Atoi(f); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}
