# Track And Re-Ask Unanswered Questions

When you ask the user a question, treat that question as open until the user directly answers it.

## Definitions

Use `.agent/DEFINITIONS.md` as the source of truth for definitions used in this repository's standalone documentation system. If a reusable term is missing, add it there instead of defining it locally in this document.

Let one coordinator own the open-question queue. The coordinator decides which question is oldest, whether a later user message answered it, and when it must be asked again.

Treat the open-question queue as a mailbox. Record the oldest unanswered question clearly enough that it can be collected back into the next reply without guessing.

If the user sends a new request before answering your open question, do the new request first.

After you complete the new request, return to your oldest open question and ask it again.

At the end of every reply, include a section named exactly: `Unanswered questions`.

If you have no open questions, write: `Unanswered questions: none`.

If you have at least one open question, list only the oldest open question as one bullet point and ask the user to answer it.

If the user's latest message directly answers your open question, quote the exact words that answer it and remove the question from the open queue.

If the user tells you to drop a question, remove it from the open queue and do not ask it again.
