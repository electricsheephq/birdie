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
  to let birdie pick a running session: one that takes pushes first, then the
  most recently active. The refusal lists the running sessions, so the sender
  can address one instead.
- **Claude Code sessions in the desktop app are now recognised.** That covers
  a path with a space in it. Those sessions start getting messages pushed to
  them, and `/clear` is tracked.
- **Expect one burst of notices on first start.** Messages held for a session
  that stopped more than 7 days ago (`serve --hold-for`) expire, and each
  sender is told once. Mail sent to bare `claude` before the upgrade, in a
  thread `claude` never spoke in or took a message in, is held too: its
  sender is told, and it expires 7 days after that unless you choose a
  default or turn fallback on first, which lets a session take it. Bare mail
  in a thread a `claude` session already took part in isn't covered yet
  (#27).
- **The database gains two columns on start:** `messages.held_at` and
  `sessions.pull_only`, both 0 by default.
- **Rolling back:** the bbcdfcc binary runs on the upgraded database and
  ignores the new columns. Messages that expired stay expired, and its
  `birdie thread` shows them as `expired`.
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
- A stalled daemon can't hold up a session's bridge forever: reporting or
  ending a session, and confirming or returning a message, each give up after
  10 seconds (#22).
- Event previews, checks for a notice already sent, and telling a sender a
  message is held no longer copy a whole message, however large (#23).
  Confirming or returning a message still reads it once, for its event.
- Mail sent to bare `claude` before the upgrade, in a thread `claude` never
  spoke in or took a message in, is no longer stranded. Its sender is told it
  is held, and it expires after the hold time, counted from then (#24).
  Answers to what `claude` asked without a session, and follow-ups a reader
  without a session can take, are left as they are. One case is still open:
  a pre-upgrade follow-up in a thread a `claude` session took. Plain
  `check_inbox` doesn't take it, and its sender isn't told (#27).
