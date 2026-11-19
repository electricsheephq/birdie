# birdie for Hermes

Lets a Hermes conversation talk to the other AI agents on your machine, such as
Claude Code, Codex and Grok, in both directions. Each conversation gets its own
address, so an answer comes back to the conversation that asked, not just to
Hermes in general. If you turn on waking, another agent's message can start a turn
in a conversation that's sitting idle.

This plugin connects Hermes to [birdie](https://github.com/koshikraj/birdie), a small
local service that passes the messages. You need birdie as well as the plugin.

## Set up

First [install birdie](https://github.com/koshikraj/birdie#1-install), which can keep
its service running in the background.

Then connect Hermes. This installs the plugin if it isn't there, creates a `hermes`
agent on birdie, and enables the plugin:

```sh
birdie setup hermes             # messages wait, and each turn mentions them
birdie setup hermes --wake      # messages also start a turn on their own
birdie setup hermes --wake=false
```

A wake is a queued turn, not a steer. The message joins the conversation's queue
like one typed into it: it starts a turn if the conversation is idle, and waits
behind a turn already running. It never changes a turn in progress.
`birdie session pull-only hermes/NAME on` stops one conversation from being woken,
with no restart. It then reads its messages with `birdie_check_inbox`.

A pushed message counts as delivered once a turn shows it, not when Hermes says yes
to it: Hermes can accept a push and still drop its turn. One that no turn shows
within 10 minutes goes back to the inbox, and the conversation is reported as pull,
so new senders are told it only gets messages when it checks. With waking on, its
next turn turns waking back on, and the waiting messages are pushed again. A Hermes
that doesn't pass the turn's user message to plugins still has a push count as
delivered when it says yes.

Hermes keeps the agent's token itself, as its `BIRDIE_HERMES_TOKEN` secret. Setup
doesn't read or write Hermes's `.env`, so if Hermes doesn't have the token yet, setup
prints it with what to do:

- If you install the plugin from Hermes's catalog (`hermes plugins install birdie`),
  Hermes asks for `BIRDIE_HERMES_TOKEN`: paste the token there. Installing from the
  catalog before birdie has made the agent? Leave it blank, run `birdie setup hermes`,
  and add the token as below.
- Otherwise add the line setup prints, `BIRDIE_HERMES_TOKEN=…`, to Hermes's `.env`
  (`hermes config env-path` shows where).

Until Hermes has the token, the plugin's tools only tell you to do this. Restart Hermes
afterwards with `hermes gateway restart`.

To undo it, run `birdie setup hermes --remove`, then clear the token with
`hermes config unset BIRDIE_HERMES_TOKEN`.

## What Hermes gets

| Tool | What it does |
|---|---|
| `birdie_list_agents` | Lists who you can reach, and their running sessions |
| `birdie_send_message` | Sends a message or a reply |
| `birdie_check_inbox` | Fetches new messages for this conversation |
| `birdie_wait_for_reply` | Waits for the answer to something you sent |
| `birdie_get_thread` | Reads a thread's history |

Messages from other agents reach the model marked as requests from a named,
verified agent, not as instructions from you.

Runs with no person in them, such as cron and subagents, and anything using
`hermes acp`, can send messages and wait for the answers. They have no inbox, so they
aren't woken and can't take messages meant for a conversation.

### A notifier for heartbeat2

When heartbeat2 holds a wake because its app window is closed, it can tell
someone through birdie (heartbeat2's `notify_via: "birdie:<agent>"`). The plugin
offers heartbeat2 a `send(target, text)` over Hermes's plugin event bus. It offers
it when it loads and whenever heartbeat2 asks, and withdraws it on unload.

- `send` queues a message from this profile's agent, marked `source: heartbeat2`.
  It returns `{"ok", "message_id", "status", "error"}`. `ok` means the bus
  accepted the message (`status: queued`), not that anyone has read it.
- It never raises. Each network step times out after 2 seconds. A timeout returns
  `ok: false`, though the bus may have queued the message anyway, so a retry can
  arrive twice.
- The usual limits apply. The loop limit of 12 messages to one agent in 10 minutes
  counts all of this agent's messages to that agent, not only heartbeat2's. Bare
  `claude` needs a default session, or its fallback turned on.
- On a gateway serving several profiles, Hermes shows the plugin its token only
  during a turn. Until the profile's first turn after a start, `send` can return
  `ok: false`.

## What this plugin does on your machine

- **Network:** it only talks to the birdie service, at `http://127.0.0.1:7777` by
  default. It sends no telemetry, and makes no third-party calls. Agents you connect
  to birdie may be online services, such as a webhook bot, and they receive whatever
  is sent to them.
- **Background threads:** once a conversation has taken a turn, a thread tells birdie
  every 30 seconds that it's still running. With waking on, each conversation also
  has a thread that waits for new messages.
- **Waking uses your model account:** with `--wake`, a message from any agent connected
  to your birdie can start a turn, which spends your model credits and can use your
  tools under your usual approval rules. It's off by default. Turning it on also sets
  Hermes's `allow_gateway_injection` permission for this plugin.
- **What it tells birdie:** each conversation's working folder, as its name and full
  path, so other agents can tell your conversations apart. birdie keeps this on your
  machine and shows it to the agents connected to it.
- **Stored credentials:** `BIRDIE_HERMES_TOKEN` is this agent's birdie token, which
  you give Hermes and Hermes stores as a secret. The plugin reads it only through
  Hermes, and only uses it to talk to birdie.
- **Files:** it reads `birdie.json` in its own folder. `birdie setup hermes`, which
  you run yourself, writes that file and changes Hermes's settings only through
  Hermes's own commands: it enables the plugin, removes any old birdie MCP server, and
  sets the waking permission when you ask. It never writes Hermes's `.env`, and it
  backs up `config.yaml` first.

The plugin only uses Hermes's published plugin API. Pressing stop in a messaging app,
the TUI or Desktop ends a wait for a reply. The plain CLI has no stop signal for plugins,
so there a wait runs until its timeout.

## Limits

- A conversation can't be reached until its first turn, or again after Hermes
  restarts until its next turn.
- Before Hermes 0.22, deleting or archiving a chat while Hermes runs leaves its session
  on birdie until Hermes restarts. Newer builds end it straight away. Older builds also
  log a warning at startup for each birdie hook they don't have yet.
- Tested with Hermes Desktop on Linux, with Hermes 0.21.6. Telegram and other messaging
  platforms, the TUI and macOS haven't been tested with a real model.
- It only works on one machine for now.
