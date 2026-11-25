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

## Release and escalation
- Lane by changed path, not by plugin (below, "the orchestrating owner" and "the human owner" are this project's owners). The careful lane covers changes to storage or migrations, deletion or compaction, locking, provider routing, streaming or retries, permissions, shared hooks, dependencies or update machinery, a host-wide install (a shared plugin root or processes, including one that can't be isolated), and anything with uncertain recovery. On top of the fast-lane proof floor below, it adds mechanism tests (old-state upgrade, restart or interruption, concurrency, recovery after new writes), a consistent backup, a recovery rehearsed on a copy that keeps post-upgrade data, and a canary on one already-authorized live install, running real operations with stated pass/fail thresholds. For an install that can't be isolated, the orchestrating owner decides the blast radius. Everything else takes the fast lane.
- Fast-lane proof floor for the smallest proven fix: an independent non-author review and green CI at the exact final SHA, targeted regression tests, and a smoke test of install, startup and the affected workflow against the current release. Reaching that proof should take hours, not days. Don't wait for GA, a release date, unrelated work or a benchmark campaign. Once it passes, release on the cadence below. A remaining P0 doesn't block the other fixes if it is contained (verified workaround, no new exposure, known-issue note, named fix owner, next-update date); otherwise the orchestrating owner decides the trade-off. In either lane, before shipping, name a watcher whose wake route has been seen to fire and a ready rollback or disable command, then watch for 72 h or one update cycle. On corruption, disclosure, an unauthorized action, a critical-path failure or a breached threshold: stop, contain, roll back or disable, tell the orchestrating owner and keep the evidence. A published release can't be recalled from auto-updating users; when that matters, ship a fast corrective release with clear user instructions.
- Tests follow the diff: if the plugin has slow or benchmark suites, keep a short map from each one to the code and behavior it measures. A release runs only the suites whose inputs changed and records each skip in the release note ("suite X skipped: nothing it measures changed"). The full suite runs on its own nightly or weekly schedule, off the release path. A regression it finds gets its own fix and never gates an unrelated hotfix.
- Severity and cadence (binding release rules for maintainers; not a support promise to users). P0 is data loss, disclosure or an unauthorized action (even for one user), or the default critical workflow broken with no safe workaround: contain a suspected P0 first and tell the orchestrating owner at once. P1 is a supported workflow blocked or materially degraded for some users, without P0 harm. An owner's priority call goes into your plan and changes the order of work, never the severity. P0 fixes ship immediately; ready P1 fixes go out in at most one bundled release a day; features ride a regular train (weekly by default, set per plugin by the orchestrating owner). Never hold a ready fix past the next daily bundle, and never cut a release that carries nothing a user needs.
- Anti-stall clocks, which never reset. A P0 with no safe path within 2 h of triage goes to the orchestrating owner. For a fix candidate with no decisive test, review or authorization result within 4 h, send the orchestrating owner the blocker, the remaining evidence, the fix owner and the next result time; a longer test needs the orchestrating owner's explicit OK, once, and a missed checkpoint re-escalates. A ready fix held 4 h needs a named blocker; send it to the orchestrating owner with a deadline for their decision. Running out of time never waives proof; it forces a decision.
- Who decides: maintainers make reversible technical choices within the permissions they already have, then report. The orchestrating owner decides cross-plugin priorities, user-impact trade-offs, unresolved high risks, blast radius and longer tests; an escalation carries the evidence, the options, a recommendation, a deadline and a safe fallback. The human owner decides new access or credentials, money, social posts in their name and destructive actions. Code posts (pull requests, issues, comments and reviews): on our own repositories the maintainer's own privacy scan is enough for the privacy check, unless the post quotes a user's message or names a person; on third-party repositories, and for any post that quotes a user's message or names a person, the orchestrating owner's privacy-checked OK comes first, and only that OK can allow a specific name or quote. Privacy means no client or personal data, names, hosts, paths, internal IDs or message text; public issue and pull request numbers are fine. This standard grants no new authority to publish, install or spend: live installs happen only in the guarded install window (backup first, then one restart), run by whoever the orchestrating owner has assigned to it, never on your own. Silence allows only a default that is already delegated and passes the gates above; it never grants new authority or accepts new risk: on silence, hold or contain, and escalate again.

Maintainer agents: the full internal standard is in eng-standards policies/release-and-escalation.md.

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
