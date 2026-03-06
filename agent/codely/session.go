package codely

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/chenhg5/cc-connect/core"
)

// codelySession manages multi-turn conversations with the Codely CLI.
// Each Send() launches a new `codely -p ... --output-format stream-json` process
// with --resume for conversation continuity.
type codelySession struct {
	cmd      string
	workDir  string
	model    string
	mode     string
	extraEnv []string
	events   chan core.Event
	chatID   atomic.Value // stores string — Codely session ID
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	alive    atomic.Bool
	initTime atomic.Value // stores time.Time — session init time
}

func newCodelySession(ctx context.Context, cmd, workDir, model, mode, resumeID string, extraEnv []string) (*codelySession, error) {
	sessionCtx, cancel := context.WithCancel(ctx)

	cs := &codelySession{
		cmd:      cmd,
		workDir:  workDir,
		model:    model,
		mode:     mode,
		extraEnv: extraEnv,
		events:   make(chan core.Event, 64),
		ctx:      sessionCtx,
		cancel:   cancel,
	}
	cs.alive.Store(true)

	if resumeID != "" {
		cs.chatID.Store(resumeID)
	}

	slog.Debug("codelySession: newCodelySession", "resumeID", resumeID)

	return cs, nil
}

func (cs *codelySession) Send(prompt string, images []core.ImageAttachment) error {
	if !cs.alive.Load() {
		return fmt.Errorf("session is closed")
	}

	// Codely CLI supports @file references for images; save to temp files
	var imageRefs []string
	if len(images) > 0 {
		tmpDir := os.TempDir()
		for i, img := range images {
			ext := ".png"
			switch img.MimeType {
			case "image/jpeg":
				ext = ".jpg"
			case "image/gif":
				ext = ".gif"
			case "image/webp":
				ext = ".webp"
			}
			fname := fmt.Sprintf("cc-connect-img-%d%s", i, ext)
			fpath := fmt.Sprintf("%s/%s", tmpDir, fname)
			if err := os.WriteFile(fpath, img.Data, 0o644); err != nil {
				slog.Warn("codelySession: failed to save image", "error", err)
				continue
			}
			imageRefs = append(imageRefs, fpath)
		}
	}

	chatID := cs.CurrentSessionID()
	isResume := chatID != ""

	args := []string{
		"--output-format", "stream-json",
	}

	switch cs.mode {
	case "yolo":
		args = append(args, "-y")
	case "auto_edit":
		args = append(args, "--approval-mode", "auto_edit")
	case "plan":
		args = append(args, "--approval-mode", "plan")
	}

	if isResume {
		args = append(args, "--resume-session", chatID)
	}

	if cs.model != "" {
		args = append(args, "-m", cs.model)
	}

	// Build the prompt with image file references
	fullPrompt := prompt
	if len(imageRefs) > 0 {
		fullPrompt = strings.Join(imageRefs, " ") + " " + prompt
	}

	// Wrap prompt in quotes to handle spaces and special characters
	args = append(args, "-p", fmt.Sprintf("%q", fullPrompt))

	slog.Debug("codelySession: launching", "args", args)

	cmd := exec.CommandContext(cs.ctx, cs.cmd, args...)
	cmd.Dir = cs.workDir
	env := os.Environ()
	if len(cs.extraEnv) > 0 {
		env = append(env, cs.extraEnv...)
	}
	cmd.Env = env

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("codelySession: stdout pipe: %w", err)
	}

	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("codelySession: start: %w", err)
	}

	cs.wg.Add(1)
	go cs.readLoop(cmd, stdout, &stderrBuf, imageRefs)

	return nil
}

func (cs *codelySession) readLoop(cmd *exec.Cmd, stdout io.ReadCloser, stderrBuf *bytes.Buffer, tempImages []string) {
	defer cs.wg.Done()
	defer func() {
		// Clean up temp image files
		for _, f := range tempImages {
			os.Remove(f)
		}
		if err := cmd.Wait(); err != nil {
			stderrMsg := strings.TrimSpace(stderrBuf.String())
			if stderrMsg != "" {
				slog.Error("codelySession: process failed", "error", err, "stderr", stderrMsg)
				cs.events <- core.Event{Type: core.EventError, Error: fmt.Errorf("%s", stderrMsg)}
			}
		}
	}()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			slog.Debug("codelySession: non-JSON line", "line", line)
			continue
		}

		cs.handleEvent(raw)
	}

	if err := scanner.Err(); err != nil {
		slog.Error("codelySession: scanner error", "error", err)
		cs.events <- core.Event{Type: core.EventError, Error: fmt.Errorf("read stdout: %w", err)}
	}
}

// Codely CLI stream-json event types (same as Gemini CLI):
//   init       — session_id, model
//   message    — role (user/assistant), content, delta
//   tool_use   — tool_name, tool_id, parameters
//   tool_result — tool_id, status, output, error
//   error      — severity, message
//   result     — status, stats (final event)
func (cs *codelySession) handleEvent(raw map[string]any) {
	eventType, _ := raw["type"].(string)

	switch eventType {
	case "init":
		cs.handleInit(raw)
	case "message":
		cs.handleMessage(raw)
	case "tool_use":
		cs.handleToolUse(raw)
	case "tool_result":
		cs.handleToolResult(raw)
	case "error":
		cs.handleError(raw)
	case "result":
		cs.handleResult(raw)
	default:
		slog.Debug("codelySession: unhandled event", "type", eventType)
	}
}

func (cs *codelySession) handleInit(raw map[string]any) {
	sid, _ := raw["session_id"].(string)
	model, _ := raw["model"].(string)
	timestamp, _ := raw["timestamp"].(string)

	if sid != "" {
		cs.initTime.Store(time.Now())

		// Try to read the actual sessionId from the session file
		// The init event contains a short ID, but the session file stores the full UUID
		time.Sleep(2 * time.Second)
		actualSessionID := cs.readSessionFileID()
		if actualSessionID != "" {
			cs.chatID.Store(actualSessionID)
			slog.Debug("codelySession: using actual session ID from file", "session_id", actualSessionID)
			sid = actualSessionID
		}

		slog.Debug("codelySession: session init", "session_id", actualSessionID, "model", model, "timestamp", timestamp)

		cs.events <- core.Event{
			Type:      core.EventText,
			SessionID: actualSessionID,
			Content:   "",
			ToolName:  model,
		}
	}
}

// readSessionFileID reads the full sessionId from the most recent session file
func (cs *codelySession) readSessionFileID() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		slog.Debug("codelySession: readSessionFileID - failed to get home dir", "error", err)
		return ""
	}

	projName := codelyProjectHash(cs.workDir)
	chatsDir := filepath.Join(homeDir, ".codely-cli", "tmp", projName, "chats")
	slog.Debug("codelySession: readSessionFileID - chats dir", "dir", chatsDir)

	entries, err := os.ReadDir(chatsDir)
	if err != nil {
		slog.Debug("codelySession: readSessionFileID - failed to read chats dir", "error", err)
		return ""
	}
	slog.Debug("codelySession: readSessionFileID - found entries", "count", len(entries))

	// Find the most recent session file by modification time
	var bestMatch struct {
		sessionID  string
		modTime    time.Time
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		filename := entry.Name()
		if !strings.HasPrefix(filename, "session-") {
			continue
		}

		// Extract session ID from filename: session-xxx.json -> xxx
		sessionID := strings.TrimSuffix(filename[8:], ".json")
		if sessionID == "" {
			continue
		}

		// Get file modification time
		info, err := entry.Info()
		if err != nil {
			continue
		}

		if bestMatch.sessionID == "" || info.ModTime().After(bestMatch.modTime) {
			bestMatch.sessionID = sessionID
			bestMatch.modTime = info.ModTime()
		}
	}

	slog.Debug("codelySession: readSessionFileID - result", "sessionID", bestMatch.sessionID)
	return bestMatch.sessionID
}

func (cs *codelySession) handleMessage(raw map[string]any) {
	role, _ := raw["role"].(string)
	content, _ := raw["content"].(string)
	delta, _ := raw["delta"].(bool)

	if role == "user" {
		return
	}

	// assistant message (may be delta or full)
	if content != "" {
		_ = delta // both delta and full messages are streamed as text events

		cs.events <- core.Event{
			Type:    core.EventText,
			Content: content,
		}
	}
}

func (cs *codelySession) handleToolUse(raw map[string]any) {
	toolName, _ := raw["tool_name"].(string)
	toolID, _ := raw["tool_id"].(string)
	params, _ := raw["parameters"].(map[string]any)

	input := formatToolParams(toolName, params)

	slog.Debug("codelySession: tool_use", "tool", toolName, "id", toolID)
	cs.events <- core.Event{
		Type:      core.EventToolUse,
		ToolName:  toolName,
		ToolInput: input,
	}
}

func (cs *codelySession) handleToolResult(raw map[string]any) {
	toolID, _ := raw["tool_id"].(string)
	status, _ := raw["status"].(string)
	output, _ := raw["output"].(string)

	slog.Debug("codelySession: tool_result", "tool_id", toolID, "status", status)

	if status == "error" {
		errObj, _ := raw["error"].(map[string]any)
		if errObj != nil {
			errMsg, _ := errObj["message"].(string)
			if errMsg != "" {
				output = "Error: " + errMsg
			}
		}
	}

	if output != "" {
		cs.events <- core.Event{
			Type:     core.EventToolResult,
			ToolName: toolID,
			Content:  truncate(output, 500),
		}
	}
}

func (cs *codelySession) handleError(raw map[string]any) {
	severity, _ := raw["severity"].(string)
	message, _ := raw["message"].(string)

	if message != "" {
		slog.Warn("codelySession: error event", "severity", severity, "message", message)
		cs.events <- core.Event{
			Type:  core.EventError,
			Error: fmt.Errorf("[%s] %s", severity, message),
		}
	}
}

func (cs *codelySession) handleResult(raw map[string]any) {
	status, _ := raw["status"].(string)

	var errMsg string
	if status == "error" {
		errObj, _ := raw["error"].(map[string]any)
		if errObj != nil {
			errMsg, _ = errObj["message"].(string)
		}
	}

	sid := cs.CurrentSessionID()

	if errMsg != "" {
		cs.events <- core.Event{
			Type:      core.EventResult,
			Content:   errMsg,
			SessionID: sid,
			Done:      true,
			Error:     fmt.Errorf("%s", errMsg),
		}
	} else {
		cs.events <- core.Event{
			Type:      core.EventResult,
			SessionID: sid,
			Done:      true,
		}
	}
}

// RespondPermission is a no-op — Codely CLI permissions are handled via -y / --approval-mode flags.
func (cs *codelySession) RespondPermission(_ string, _ core.PermissionResult) error {
	return nil
}

func (cs *codelySession) Events() <-chan core.Event {
	return cs.events
}

func (cs *codelySession) CurrentSessionID() string {
	v, _ := cs.chatID.Load().(string)
	return v
}

func (cs *codelySession) Alive() bool {
	return cs.alive.Load()
}

func (cs *codelySession) Close() error {
	cs.alive.Store(false)
	cs.cancel()
	cs.wg.Wait()
	close(cs.events)
	return nil
}

// formatToolParams extracts a human-readable summary from tool parameters.
func formatToolParams(toolName string, params map[string]any) string {
	if params == nil {
		return ""
	}

	switch toolName {
	case "shell", "run_shell_command":
		if cmd, ok := params["command"].(string); ok {
			return truncate(cmd, 200)
		}
	case "write_file", "read_file", "replace":
		if p, ok := params["file_path"].(string); ok {
			return p
		}
		if p, ok := params["path"].(string); ok {
			return p
		}
	case "web_fetch":
		if u, ok := params["url"].(string); ok {
			return truncate(u, 200)
		}
	case "google_web_search":
		if q, ok := params["query"].(string); ok {
			return truncate(q, 200)
		}
	}

	b, _ := json.Marshal(params)
	return truncate(string(b), 200)
}

func truncate(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}
	return string([]rune(s)[:maxRunes]) + "..."
}
