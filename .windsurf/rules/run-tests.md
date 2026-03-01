---
trigger: always_on
---

After implementing a feature or modifying code you must run tests to validate your changes before continuing. 

## Test Workflow

Follow these steps to validate your changes to the codebase. Do not skip steps.

1. Run the command:

```
mix test --seed 0 --trace
```

2. Wait for the command to complete. 

3. Do not rush to a conclusion. When warnings or errors appear, stop and make a plan to fix them.

4. Do not silence the problem by disabling a test or tool, skipping a check, or suppressing the warning. Do not work around the error. Investigate until you identify the root cause, then fix the root cause.

5. Follow these steps to investigate a problem:

- Break it down into smaller, manageable parts
- Examine it from multiple angles
- Consider all possible causes, not just the most obvious one
- Don't stop at the first solution that seems to work
- Assume that any part of the current situation might be wrong.
- Be skeptical of your starting point, including the test that failed, the error message, and any assumptions you are carrying forward.
- Reason from the purpose of the change and the user’s real-world use case.

6. After you have explored the plausible causes and approaches, propose two solutions that are most likely to be correct. Explain why you believe those two are the best options and ask the user to choose an option. Wait for the user to choose an option.

7. Once the user has chosen an option, implement the chosen solution and start from 1. Repeat this process until all warnings and errors are fixed.