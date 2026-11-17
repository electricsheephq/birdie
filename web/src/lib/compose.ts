// Hand-offs to the reply box under a thread in Messages.

/** The thread whose reply box takes focus when it opens, after a send from Agents. */
let focusThread = '';

/** Focuses the reply box of `thread` the next time it shows. */
export function focusReplyIn(thread: string) {
  focusThread = thread;
}

/** Whether the reply box of `thread` should take focus now; asking uses it up. */
export function takeFocus(thread: string): boolean {
  if (focusThread !== thread) return false;
  focusThread = '';
  return true;
}

/** System senders, whose notices can't be answered. */
export function isSystem(agent: string): boolean {
  return agent === 'birdie' || agent === 'agent-bus';
}

/**
 * The agent a message starting with "@name" is for: its name, or its label
 * without spaces, in any case. Returns '' when there is no mention, and the
 * name as typed, prefixed with '?', when no agent matches.
 */
export function mentionIn(text: string, agents: { name: string; label: string }[]): string {
  const m = /^@([\p{L}\p{N}._-]+)/u.exec(text);
  if (!m) return '';
  const want = m[1].toLowerCase();
  const hit = agents.find((a) => a.name.toLowerCase() === want || a.label.replace(/\s+/g, '').toLowerCase() === want);
  return hit ? hit.name : '?' + m[1];
}
