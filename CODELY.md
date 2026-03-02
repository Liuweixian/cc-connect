# cc-connect

## Project Overview

cc-connect is a Go-based bridge application that connects local AI coding assistants (Claude Code, Cursor, Gemini CLI, Codex, Codely) to messaging platforms (Feishu, DingTalk, Slack, Telegram, Discord, LINE, WeChat Work, QQ). The application enables bidirectional communication with AI agents from anywhere, with most platforms requiring no public IP.

### Architecture

The application follows a three-component architecture:

- **Platform** — Messaging platform adapter that handles receiving/sending messages via WebSocket, Stream, Long Polling, etc.
- **Agent** — AI assistant adapter that invokes local AI tools and collects responses
- **Engine** — Core router that manages sessions, routes messages between platforms and agents, and handles slash commands

All components are decoupled via Go interfaces, making the system fully pluggable and extensible.

### Technology Stack

- **Language:** Go 1.24.2
- **Configuration:** TOML (github.com/BurntSushi/toml)
- **Key Dependencies:**
  - bwmarrin/discordgo - Discord Gateway
  - go-telegram-bot-api/v5 - Telegram Bot API
  - gorilla/websocket - WebSocket support
  - larksuite/oapi-sdk-go/v3 - Feishu/Lark SDK
  - line/line-bot-sdk-go/v8 - LINE Bot SDK
  - open-dingtalk/dingtalk-stream-sdk-go - DingTalk Stream SDK
  - slack-go/slack - Slack API
  - robfig/cron/v3 - Cron scheduling

## Building and Running

### Prerequisites

- Go 1.22+ (or 1.24.2 as specified in go.mod)
- For speech-to-text: ffmpeg installed

### Build Commands

```bash
# Build the binary
make build

# Build and run
make run

# Run tests
make test

# Run linter
make lint

# Build for all platforms (creates distributions in dist/)
make release-all

# Build for specific platform
make release TARGET=linux/amd64

# Clean build artifacts
make clean
```

### Running the Application

```bash
# Run with default config (./config.toml → ~/.cc-connect/config.toml)
./cc-connect

# Run with explicit config path
./cc-connect -config /path/to/config.toml

# Show version
./cc-connect --version

# Self-update binary
cc-connect update

# Check for updates
cc-connect check-update
```

### Configuration

The application uses TOML configuration files. Config search order:
1. `-config <path>` flag (explicit)
2. `./config.toml` (current directory)
3. `~/.cc-connect/config.toml` (global, recommended)

If no config exists, running `cc-connect` auto-creates a starter template.

Example config structure:
```toml
[log]
level = "info"

[[projects]]
name = "my-project"

[projects.agent]
type = "claudecode"  # "claudecode", "codex", "cursor", "gemini", or "codely"

[projects.agent.options]
work_dir = "/path/to/project"
mode = "default"

[[projects.platforms]]
type = "feishu"

[projects.platforms.options]
app_id = "your-app-id"
app_secret = "your-app-secret"
```

### CLI Subcommands

```bash
# Provider management
cc-connect provider add --project <name> --name <provider> --api-key <key>
cc-connect provider list --project <name>
cc-connect provider remove --project <name> --name <provider>

# Send message to session
cc-connect send --project <name> --session-key <key> --message "msg"

# Cron job management
cc-connect cron add --cron "0 6 * * *" --prompt "task" --desc "label"
cc-connect cron list
cc-connect cron del <job-id>
```

## Development Conventions

### Project Structure

```
cc-connect/
├── cmd/cc-connect/          # Main entry point
│   ├── main.go              # Application initialization and startup
│   ├── cron.go              # Cron subcommand implementation
│   ├── provider.go          # Provider management subcommand
│   ├── send.go              # Send message subcommand
│   └── update.go            # Self-update functionality
├── core/                    # Core abstractions
│   ├── interfaces.go        # Platform and Agent interfaces
│   ├── registry.go          # Plugin-style factory registry
│   ├── message.go           # Unified message/event types
│   ├── session.go           # Multi-session management
│   ├── engine.go            # Routing engine + slash commands
│   ├── speech.go            # Speech-to-text (Whisper API + ffmpeg)
│   ├── cron.go              # Cron scheduler
│   └── api.go               # Internal API server for CLI commands
├── platform/                # Platform adapters
│   ├── feishu/              # Feishu/Lark (WebSocket)
│   ├── dingtalk/            # DingTalk (Stream)
│   ├── telegram/            # Telegram (Long Polling)
│   ├── slack/               # Slack (Socket Mode)
│   ├── discord/             # Discord (Gateway WebSocket)
│   ├── line/                # LINE (HTTP Webhook)
│   ├── wecom/               # WeChat Work (HTTP Webhook)
│   └── qq/                  # QQ (NapCat/OneBot v11 WebSocket)
├── agent/                   # Agent adapters
│   ├── claudecode/          # Claude Code CLI (interactive sessions)
│   ├── codely/              # Codely CLI (Qwen fork of Gemini CLI)
│   ├── codex/               # OpenAI Codex CLI (exec --json)
│   ├── cursor/              # Cursor Agent CLI (--print stream-json)
│   └── gemini/              # Gemini CLI (-p --output-format stream-json)
├── config/                  # Configuration handling
├── docs/                    # Platform setup guides
└── config.example.toml      # Config template
```

### Adding a New Platform

1. Implement the `core.Platform` interface:
   ```go
   type Platform interface {
       Name() string
       Start(handler MessageHandler) error
       Reply(ctx context.Context, replyCtx any, content string) error
       Send(ctx context.Context, replyCtx any, content string) error
       Stop() error
   }
   ```

2. Register the platform via init():
   ```go
   func init() {
       core.RegisterPlatform("myplatform", New)
   }
   ```

3. Add blank import in `cmd/cc-connect/main.go`:
   ```go
   _ "github.com/chenhg5/cc-connect/platform/myplatform"
   ```

### Adding a New Agent

1. Implement the `core.Agent` interface:
   ```go
   type Agent interface {
       Name() string
       StartSession(ctx context.Context, sessionID string) (AgentSession, error)
       ListSessions(ctx context.Context) ([]AgentSessionInfo, error)
       Stop() error
   }
   ```

2. Implement `AgentSession` interface for running sessions:
   ```go
   type AgentSession interface {
       Send(prompt string, images []ImageAttachment) error
       RespondPermission(requestID string, result PermissionResult) error
       Events() <-chan Event
       CurrentSessionID() string
       Alive() bool
       Close() error
   }
   ```

3. Register via `core.RegisterAgent("myagent", New)`

4. Add blank import in `cmd/cc-connect/main.go`

### Optional Interfaces

Platforms/Agents can implement optional interfaces for extended functionality:

- `ReplyContextReconstructor` - Recreate reply context for cron jobs
- `SessionEnvInjector` - Accept per-session environment variables
- `MessageUpdater` - Support updating messages
- `ToolAuthorizer` - Dynamic tool authorization
- `HistoryProvider` - Retrieve conversation history
- `ProviderSwitcher` - Multiple API provider support
- `MemoryFileProvider` - Persistent instruction files
- `ModeSwitcher` - Runtime permission mode switching

### Coding Style

- Standard Go conventions (gofmt)
- Use slog for structured logging
- Context propagation for async operations
- Error handling with explicit error returns
- Interface-based design for extensibility

### Testing

Currently, no test files are present in the codebase. When adding tests:
- Use `*_test.go` naming convention
- Run with `make test`
- Test files should be co-located with source files

### Linting

The project uses golangci-lint:
```bash
make lint
```

## Key Features

### Permission Modes

All agents support runtime permission mode switching via `/mode`:
- **Claude Code:** default, acceptEdits (edit), plan, bypassPermissions (yolo)
- **Codex:** suggest, auto-edit, full-auto, yolo
- **Cursor:** default, force (yolo), plan, ask
- **Gemini:** default, auto_edit (edit), yolo, plan
- **Codely:** default, auto_edit (edit), yolo, plan

### API Provider Management

Switch between API providers (Anthropic, relay services, AWS Bedrock, etc.) at runtime via `/provider` command or CLI. Provider credentials are injected as environment variables.

### Speech-to-Text

Voice message transcription using Whisper API (OpenAI or Groq) with ffmpeg for audio format conversion.

### Scheduled Tasks (Cron)

Create scheduled tasks that run automatically and send results to chat sessions. Supports both CLI commands and natural language scheduling via agents.

### Session Management

Each user gets an independent session with full conversation context. Manage sessions via slash commands: `/new`, `/list`, `/switch`, `/current`, `/history`, `/stop`, `/help`.

## Documentation

- `README.md` - User-facing documentation
- `README.zh-CN.md` - Chinese version
- `INSTALL.md` - AI-agent-friendly installation guide
- `config.example.toml` - Fully commented configuration template
- `docs/` - Platform-specific setup guides (feishu.md, dingtalk.md, telegram.md, etc.)