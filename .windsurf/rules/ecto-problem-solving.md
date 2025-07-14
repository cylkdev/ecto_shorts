---
trigger: model_decision
description: Invoke this rule whenever tackling design questions for an Ecto library or complex Ecto feature: clarify goals, propose at least two fact-checked options with trade-offs, then supply a concise, numbered implementation plan.
---

Rule: Ecto Problem-Solving

Summary

When assisting with an Ecto library, always deliver architect-grade guidance: clarify goals, propose multiple design options, and verify every technical claim in official Ecto docs or source code before final advice.

Workflow

Confirm requirements (performance, migration strategy, DB features, compile-time vs runtime flexibility).

Generate at least two Ecto-centric solutions, each with pros / cons.

Fact-check unclear details in:

• hexdocs.pm/ecto (stable guide)
• Ecto changelog or GitHub source

Present the options in concise, numbered bullets so the user can choose.

After user selection, outline a step-by-step implementation plan (schemas, queries, changesets, tests).

Design Guidelines

• Default to plain Ecto functions and DSL; suggest custom macros only when they give clear compile-time wins.
• Keep any macro wrapper minimal and delegate heavy logic to normal functions.
• Highlight trade-offs: compile-time safety vs compile speed, raw SQL fragments vs Ecto DSL, embedded schemas vs references.
• Quote exact module names or code snippets when referencing Ecto APIs so they can be copy-pasted.
• If a feature changed after Ecto 3.12, note the minimum version required.

Output Style

• Use short bullet lists or numbered steps.
• Label each option (e.g., “Option 1: Pure DSL”, “Option 2: Macro Helper”).
• Provide code blocks only when they illuminate the decision.
• Avoid redundant commentary; focus on actionable differences.