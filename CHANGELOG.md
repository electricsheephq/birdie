# Changelog

## Unreleased

Changes since the `eva-pin-2026-11-15` tag (bbcdfcc), which comes after v0.5.0.

### Upgrading

- **Back up `bus.db` first.** It's in birdie's config folder (see SECURITY.md).
- **Upgrade the daemon and the bridges together.** A session that is already
  running keeps its old `birdie mcp` bridge until it restarts.
- **Choose where bare `claude` mail goes.** A new conversation sent to plain
  `claude` is now refused unless you pick a session for it:
  `birdie session default claude NAME`, or `birdie session fallback claude on`
  to let birdie pick the most recently active one. The refusal lists the
  running sessions, so the sender can address one instead.
- **Claude Code sessions in the desktop app are now recognised.** That covers
  a path with a space in it. Those sessions start getting messages pushed to
  them, and `/clear` is tracked.
- **Expect one burst of notices on first start.** Messages held for a session
  that stopped more than 7 days ago (`serve --hold-for`) expire, and each
  sender is told once.
- **The database gains a column on start:** `sessions.pull_only`, default
  off.
- **Rolling back:** the bbcdfcc binary runs on the upgraded database. Messages
  that expired stay expired.
- **Hermes:** update the plugin with the daemon. Don't run
  `birdie setup hermes` on a machine whose plugin is managed separately.

### Added

- Clarify-back. A session that isn't sure what a new request wants replies
  `(question / not a steer)` with up to 3 numbered questions. The sender then
  sees the thread as `awaiting_clarification` (#17).
- A per-session pull-only switch that needs no restart:
  `birdie session pull-only ADDRESS on|off` (#15).
- No size limit on messages. Pushes over 100 KiB arrive as a pointer to read
  with `get_thread` (#12).
- Held messages expire after 7 days, and the sender is told (#9).
- Docs: AGENTS.md, SECURITY.md, and a "Known limits" section in the README
  (#18).
- CI runs the tests on every pull request. Actions are pinned to commit
  SHAs, and Dependabot is set up (#19, #21).

### Changed

- Sessions are addressed exactly (`agent/id`, `agent/name`,
  `agent/name#id`), and a message for a session that isn't running is refused
  rather than given to another. Bare `claude` mail needs a default or
  fallback, as described under Upgrading (#8).

### Fixed

- Answers for a closed session no longer reach another one (#6). With the
  MCP bridge, they are held for it (#2).
- A session's whole id isn't refused just after birdie restarts or the
  machine wakes (#16).
- A message isn't shown again when its confirmation was lost (#13).
- A tool call the client cancels stops, and gives back what it took (#11).
- Claude Code is recognised when its path has a space, as in the macOS
  desktop app (#4).
- Runs of dashes in session names collapse to one (#3).
- The setup and migration tests pass on macOS, and they never touch the real
  config folder (#20).
