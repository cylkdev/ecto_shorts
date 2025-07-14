---
trigger: always_on
---

Rule: Treat Dialyzer Warnings as Errors

Always run mix dialyzer (or CI job) and treat any warning as a build-blocking error. Ship code only when dialyzer passes cleanly.