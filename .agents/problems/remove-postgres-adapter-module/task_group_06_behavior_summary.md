# Task Group 06 Behavior Summary

## Expected behavior (no functional change)
- `EctoShorts.QueryBuilder.Dynamics` builds the same `dynamic/2` predicates as before for scalar, array, and common operators.
- Scalar vs array semantics remain separated via `Postgres.Compiled.{Scalar,Array}` dispatch.

## API/structure changes
- `EctoShorts.QueryBuilder.Dynamics.Compiler.ClauseSpec` no longer contains `:kind`.
- Adapter implementations of `use EctoShorts.QueryBuilder.Dynamics.Compiler` no longer implement `emitter_module/0`.
- Clause generation is fixed to `dynamic_field_expr/3` and owned by the library (no configurable emitter module).

## Notes
- The compiler still generates separate compiled modules (`*.Compiled.Common/Scalar/Array`) for internal dispatch, but no “kind” tag is threaded through function arguments or stored on specs.

