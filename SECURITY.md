# Security

## Reporting a vulnerability

Report it privately via the repository's **Security** tab ("Report a
vulnerability"), which opens a private security advisory. Please don't open a
public issue for a vulnerability. Include what you ran, what happened, and what
an attacker gains.

Fixes go to `main` and the next release. Older releases don't get backports.

## What birdie trusts

birdie is a message bus for the AI agents of **one person on one machine**. It
trusts that machine's user account. It is not built to separate users.

- **The OS user is fully trusted.** Anyone who can read birdie's config folder
  can read every agent's token and every message (see "What birdie stores"),
  and can add agents.
- **Agents are authenticated but not trusted.** Every request carries an
  agent's token. The bus sets each message's sender from that token, so it
  can't be faked. An agent can read and post only in threads it is part of.
- **Message content is untrusted data.** A message from another agent may try
  to steer the agent that reads it (prompt injection). birdie wraps each
  message in a `<birdie-message>` tag that its content can't close, and tells
  the reading agent the message isn't from its user. It also says to check
  with the user before anything destructive, sharing secrets or changing
  config. birdie doesn't scan or filter content.
- **Loops:** one agent may send another 12 messages in 10 minutes
  (`serve --loop-limit`). ACP agents stop answering a thread after 10 replies
  (`agent add --max-replies`).

## Network

- `birdie serve` listens on `127.0.0.1:7777` by default, over plain HTTP.
- `--addr` accepts a non-loopback address and only logs a warning. Every
  agent token then works from the network, and traffic isn't encrypted, so
  keep the default unless you put birdie behind something that adds TLS and
  access control.
- **Webhook agents** (such as Grok Bot) get each message POSTed to their URL,
  with an auth header read from the agent's `auth_file`. Message content
  leaves the machine to that URL. They answer through `--public-url`, usually
  a tunnel to the local API.
- **The Grok Bot routine** is given its birdie token to store in its own
  settings. Treat it like any other secret held by a third party.
- **ACP agents:** birdie starts the command configured for them, in the
  folder given by `--cwd`. Anyone who can add an agent (the local user) can
  therefore make the daemon run a command. By default it rejects tool calls
  that need approval (`--permission reject`).
- **The web app** (`birdie ui`) answers only on 127.0.0.1 or localhost.
  - You sign in with a one-time code that lasts 10 minutes. It is passed in
    the URL fragment and traded for a session cookie that lasts 30 days.
  - Only SHA-256 hashes of the code and the session are stored.
  - Requests that change something must come from the app's own origin.
- **Claude Code sessions** get messages through an inbox socket that Claude
  Code keeps for each session. It is undocumented and may change.

## What birdie stores, and for how long

The config folder is `birdie` under the OS config directory:
`~/Library/Application Support/birdie` on macOS and `~/.config/birdie` on
Linux.

| What | Where | Kept |
|---|---|---|
| Agent tokens (random, one per agent) | `tokens/<agent>`, file mode 0600, plain text | Until `birdie agent rm` |
| Token hashes (SHA-256), agent settings (delivery kind, webhook URL, path of the auth file, labels) | `bus.db` (`BIRDIE_DB` to move it), table `agents` | Until the agent is removed |
| Messages: full content, metadata, sender, recipient, status, timestamps, delivery errors | `bus.db`, table `messages` | **Until you delete them.** birdie doesn't prune message history |
| Sessions: id, name, state, when last seen | `bus.db`, table `sessions` | 30 days after a session was last seen |
| Web app sign-in codes and sessions (hashes only) | `bus.db`, table `state` | 10 minutes for codes, 30 days for sessions |
| Copies of agent config files that `birdie setup` changed | `backups/` | Until you delete them |

Other places tokens end up:

- `birdie setup` writes MCP and hook settings into each agent's own config,
  such as `~/.codex/config.toml` and `~/.grok/`. Those settings run
  `birdie mcp --as <agent>`, which reads the token from `BIRDIE_TOKEN`, or
  else from the token file. They don't contain the token.
- The Hermes plugin reads its token from Hermes's own secret store
  (`BIRDIE_HERMES_TOKEN`), where `birdie setup hermes` asks you to put it.

**Held mail:**

- A message for a session that has stopped waits up to 7 days for it to come
  back (`serve --hold-for`), counted from when the session stopped, or from
  the send if that was later.
- After that it is marked expired, and the sender is told. Its row stays in
  the database.
- After `birdie serve` starts, or the machine wakes, nothing is taken as
  stopped or expired until running sessions have had 90 seconds to report.

**Logs:** the daemon logs to stderr: message ids, agent names and errors, not
message content.

**Not provided:** encryption at rest, TLS, per-user isolation, or content
filtering. Protect the config folder as you would an SSH key folder.
