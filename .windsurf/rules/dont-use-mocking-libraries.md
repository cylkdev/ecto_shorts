---
trigger: always_on
---

Rule: Avoid Mocking Frameworks

Summary

Do not rely on external mocking libraries. Instead, design code with explicit behaviours and inject real or in-memory implementations in tests.

Guidelines

• Define a behaviour module for any external dependency.
• Provide a production implementation and lightweight test implementation (stub, fake, or in-memory store).
• Use pattern matching or config to swap implementations; avoid runtime patching.
• Keep tests deterministic by asserting on returned values or messages, not on mocked call history.