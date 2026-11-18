# Vision

## Goal

The AI agents on one machine can talk to each other the way people in a team
do. Any agent can ask another a question, hand it a task, or answer it. The
answer comes back to the conversation that asked, not just to the agent in
general. A message is never lost or silently dropped: it is delivered, held
for its session with the sender told, or refused with a reason the sender can
act on.

This fork follows upstream [birdie](https://github.com/koshikraj/birdie). It
adds fixes found while running birdie as the always-on bus between several
agents and many sessions on one machine.

## Users

- **People who run several agents on one machine**, such as Claude Code,
  Codex, Grok CLI and Hermes, plus webhook agents like Grok Bot. They want the
  agents to work together without copying text between windows.
- **The agents themselves.** Every message has a verified sender and a thread.
  Each session has its own address, so an agent can reach the right
  conversation, and a message from another agent arrives as a request, not as
  an instruction from the user.

## Coverage on every Hermes surface

The Hermes plugin makes each Hermes conversation its own birdie session once it
has taken a turn. What a surface gets depends on whether it is a conversation
with a person in it:

| Hermes surface | Inbox (messages for this conversation) | Woken by a message (with `--wake`) | Sends and waits for answers |
|---|---|---|---|
| Desktop (`hermes serve`) | yes | yes | yes; pressing stop ends a wait |
| Telegram, Discord, Slack, WhatsApp, Signal, Matrix | yes | yes | yes; pressing stop ends a wait |
| Other messaging platforms | yes | no | yes |
| TUI | yes | yes | yes; pressing stop ends a wait |
| Plain CLI | yes | yes | yes; a wait runs until its timeout, since the CLI has no stop signal for plugins |
| API server | no | no | yes |
| Cron, subagents, kanban, webhooks, `hermes acp` | no | no | yes |

Without `--wake`, a conversation reads its messages with `birdie_check_inbox`,
and each turn mentions how many are waiting. So far the plugin has been tested
with a real model only in Hermes Desktop on Linux, with Hermes 0.21.6. The
plugin's [README](integrations/hermes/README.md) has the details and limits.
Claude Code, Codex and Grok connect through birdie's MCP bridge instead; the
main README's "How messages reach sessions" section covers them.

## Non-goals

- **More than one machine.** Agents elsewhere can only be webhook agents.
  There is no TLS and no cross-machine routing.
- **Steering a running turn.** A wake is a queued turn. A message never changes
  a turn in progress.
- **Deciding what an agent may do.** birdie decides only who may message whom
  (`Bus.admit`). What an agent does with a request stays under that agent's own
  rules and approvals.
- **A task scheduler or workflow engine.** birdie passes messages. Agents
  decide what to do with them.
- **Telemetry or third-party calls.** birdie and the Hermes plugin talk only to
  each other on 127.0.0.1, and to the webhook agents you add.
- **Upstream's feature plans.** Features belong to upstream. This fork's
  roadmap ([issue #40](https://github.com/electricsheephq/birdie/issues/40))
  tracks only its own fixes.
