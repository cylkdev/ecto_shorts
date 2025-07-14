---
trigger: always_on
---

Rule: Run Tests from the App Directory

Summary

Execute mix test from the Mix project that owns the code being tested to keep configs, coverage, and failures scoped correctly.

Guidelines

• Standard applications: run tests at the project root.
• Umbrella applications: run tests inside each individual app directory, not the umbrella root.