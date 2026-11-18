# Working on birdie

Notes for coding agents (and people) changing this repository. The README's
"How it fits together" section maps the code.

## Build and test

```sh
make test                 # go vet ./... and go test -race ./...
go test ./internal/...    # quick: bus, store, HTTP API, MCP server, adapters, web app API
go test ./cmd/birdie -run TestHermesPlugin   # the Hermes plugin's Python tests (needs python3)
make build                # bin/birdie
make web                  # the web app (Node 22.12+), embedded into the binary
```

- Go version: `go.mod`. The binary is built with `CGO_ENABLED=0`; SQLite is
  the pure-Go `modernc.org/sqlite`.
- birdie's folder is `os.UserConfigDir()`: `~/.config` (or
  `XDG_CONFIG_HOME`) on Linux, `~/Library/Application Support` on macOS. In
  tests, set a temporary `HOME` first and look it up the same way.
- Tests make their own temporary `HOME` and database. They never touch your
  real bus.

## Don't touch a real installation while developing

- Don't run `birdie onboard`, `birdie setup …` or `birdie service install`
  against your real home folder. They rewrite real agent configs (Claude,
  Codex, Grok, Hermes) and register a service. To try them, set `HOME` to a
  temporary folder.
- Never print, log or commit a token. Tokens live in birdie's config folder
  under `tokens/` (see SECURITY.md). Tests use made-up tokens such as
  `claude-tok`.
- Don't point a dev build at the live `bus.db`. Use `--db` or `BIRDIE_DB` with
  a scratch path.

## Conventions

- **A fix comes with a test that fails without it.** Check that it does.
- Test names say what should happen, as a sentence: `TestARestartLosesAndRepeatsNothing`.
- Text that people or agents read is plain and says what to do next, as in
  "send to a running one", not "invalid state". Errors that the API returns
  carry a code (`recipient_session_ended`) and a sentence.
- Comments say why, not what.
- `internal/bus` has no transport code. Transports call `Bus.Send` and
  `Bus.Receive`, and outbound delivery goes through a `bus.Deliverer`.
- Who may message whom is decided in one place, `Bus.admit`.

## Text that agents read lives in several places

When you change how agents should talk to each other, change every copy:

| Where | What |
|---|---|
| `internal/mcpserver/server.go` | MCP instructions, `MessageText` (how a message is shown to Claude, Codex and Grok) |
| `internal/mcpserver/host.go` | `PushText`: long messages pushed as a pointer |
| `integrations/hermes/__init__.py` | `frame()` and the tool descriptions for Hermes |
| `skills/talk-to-agents/SKILL.md` | the skill `birdie setup` installs for Claude, Codex and Grok |
| `docs/grok-bot-routine.md` and `internal/adapter/webhook.go` | Grok Bot's routine and the webhook envelope |
| `README.md` | the user-facing description |

## Security-sensitive code

Get a second review for changes to:

- `internal/httpapi` (token auth);
- `internal/webui/auth.go` (sign-in, cookies, origin checks);
- `Bus.admit` and thread access in `internal/bus`;
- `internal/adapter` (webhook auth headers, the commands ACP starts);
- the listen-address check in `cmd/birdie/main.go`.

SECURITY.md describes what birdie trusts and what it stores.

## Pull requests

- One change per pull request, opened as a draft. The body says what changed
  and why, and how it was tested.
- The installed Hermes plugin and daemon are updated by whoever runs the
  machine, not by the pull request's author.
- main requires a pull request and two checks from `.github/workflows/test.yml`:
  `go` (`make test`) and `web` (`make web`). If you rename either job, tell
  whoever runs the machine before the pull request merges, because the
  ruleset names them.

## Release closeout

This fork ships from main: a fix reaches users when whoever runs the machine
installs a main commit (the daemon, the Hermes plugin, or both). Here a
confirmed install of a main commit counts as a release, and so does a tag.
After each one:

1. Close every issue it fully served, with a comment naming the commit or tag.
   Never close an umbrella or parent issue whose own acceptance isn't met.
2. Tick the boxes it completed on the roadmap issue (#40), on tracking issues,
   and on closed issues whose checklists shipped. Close a milestone once all
   its issues are closed.
3. Update every README, CHANGELOG or roadmap line that names the latest
   release or what ships next.
4. Send whoever runs the machine one closeout line: issues closed, boxes
   ticked, docs updated (or "none").
