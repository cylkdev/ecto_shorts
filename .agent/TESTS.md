## Guidelines

- Prefer assertions that reflect what the caller observes.

- Assert on the full returned value, or on a small explicit structure you build from it, instead of asserting on many individual fields one by one.

- Use per-field assertions only when one field is the only important behavior you need to prove.