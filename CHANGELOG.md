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
  thread `claude` never spoke in or read without a session, is held too: its
  sender is told, and it expires 7 days after that unless you choose a
  default or turn fallback on first, which lets a session take it (#24,
  #30).
- **The database gains two columns on start:** `messages.held_at` and
  `sessions.pull_only`, both 0 by default.
- **Rolling back:** the bbcdfcc binary runs on the upgraded database and
  ignores the new columns. Messages that expired stay expired, and its
  `birdie thread` shows them as `expired`.
- **Hermes:** update the plugin with the daemon. Don't run
  `birdie setup hermes` on a machine whose plugin is managed separately.

### Added

- `send_message` accepts `p0`, `p1`, or `p3`. Claude Code offers P0
  immediately, shows P1 at turn boundaries, and batches P3 into digests.
  Unranked messages keep their existing delivery. Claude Code's `check_inbox`
  can read held messages while pushes are on. Claude Code push frames carry up
  to 256 KiB of rendered text and return overflow to the queue. P0 bypasses
  the loop guard and still counts toward it. Oversized P3 messages show a
  pointer preview in the activity feed.

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
- The Hermes plugin offers heartbeat2 a notifier. When heartbeat2 holds a wake
  because its app window is closed, it can tell someone through birdie
  (`notify_via: "birdie:<agent>"`). Nothing is sent unless heartbeat2 uses
  it. The plugin's README describes the handshake (#31).

### Changed

- Sessions are addressed exactly (`agent/id`, `agent/name`,
  `agent/name#id`), and a message for a session that isn't running is refused
  rather than given to another. Bare `claude` mail needs a default or
  fallback, as described under Upgrading (#8).
- The web app is built with Vite 8 (was 5.4), which clears the 18 security
  alerts on Vite's dev server. One picomatch advisory in Vite's own build and
  dev server, not in the app, has no fix yet. Building it needs Node.js 22.12
  or later. The app looks and works the same, and it now targets browsers from
  Chrome 111, Firefox 114 and Safari 16.4 on (#36).

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
- When Hermes reloads the birdie plugin, the old copy no longer reports the
  session. It could show the live session as pull for up to 30 seconds, so
  senders were told it only gets messages when it checks. A conversation that
  ended is no longer brought back the same way (#37).
- A message pushed into a Hermes conversation counts as delivered only once a
  turn shows it. Hermes says yes when it schedules the turn, and can still
  drop the turn after that, for example on a shared group chat whose session
  was just reset. Such a message was marked delivered and never shown. Now, if
  no turn shows it within 10 minutes, it goes back to the inbox, and the
  conversation is reported as pull until its next turn, so new senders are
  told it only gets messages when it checks. This needs a Hermes that passes
  the turn's user message to plugins; with an older one, a message counts as
  delivered when Hermes says yes, as before.
- A push that was still waiting for its turn when Hermes restarted or reloaded
  the plugin is confirmed when that turn shows it in full, instead of being
  put in a second time. A turn's hook also gives up on a confirmation after 2
  seconds, and stops at the first one that fails, so a bus that hangs holds
  the turn up much less; the message comes back and is confirmed then (#48).
- The setup and migration tests pass on macOS, and they never touch the real
  config folder (#20).
- A stalled daemon can't hold up a session's bridge forever: reporting or
  ending a session, and confirming or returning a message, each give up after
  10 seconds (#22). Once one of a batch's confirmations can't reach the
  daemon, the bridge stops trying the rest: they go back to the queue when
  their leases end, so a stalled daemon holds a batch, and the bridge's exit,
  for one deadline instead of one per message. A reply that stalls while the
  daemon gives its reason counts as a stall, not a refusal (#26).
- `check_inbox` and `wait_for_reply` don't show again a message the same
  conversation was shown before whose confirmation was lost: they confirm it
  and go on waiting for something new, as the push path already did. After
  `/clear`, the new conversation is shown it. A bridge that restarted in
  between doesn't know it showed it (#26).
- Event previews, checks for a notice already sent, and telling a sender a
  message is held no longer copy a whole message, however large (#23).
  Confirming or returning a message still reads it whole, once.
- Mail sent to bare `claude` before the upgrade, in a thread `claude` never
  spoke in or took a message in, is no longer stranded. Its sender is told it
  is held, and it expires after the hold time, counted from then (#24).
  Answers to what `claude` asked without a session, and follow-ups a reader
  without a session can take, are left as they are.
- Mail for no session in a thread a `claude` session took a message in, and
  `claude` hasn't spoken in, is held the same way once it has waited 2
  minutes, and its sender is told. That covers, for example, a bare
  follow-up from before the upgrade if the database was written by v0.4.0 or
  later (earlier versions didn't record which session took a message). A
  session that waits on the thread still takes it (#30).
- A `thread_id`-only follow-up from someone the recipient never exchanged a
  message with in that thread now goes to the session the recipient last sent
  from, or took a message with, there. Before, it was queued for no session:
  `claude` got it only from a session waiting on the thread or a reader
  without one, and an agent that picks its own session gave it to its newest
  conversation, often an unrelated one. A follow-up from someone the
  recipient did exchange messages with goes where it went before. If the
  recipient last took part without a session, the follow-up stays for no
  session, as before. If that session has stopped, the follow-up is held for
  it like an answer, and its sender is told (#29).
