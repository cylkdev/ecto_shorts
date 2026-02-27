# Code Smells Index for Refactoring

Use this document to classify the primary smell before selecting a refactoring technique.

## How to Use This Index

1. Pick the dominant smell in the current code.
2. Open the linked smell document and confirm the symptoms.
3. Choose one or more techniques from `agent/refactors/techniques/AGENTS.md` that directly target that smell.
4. Apply the smallest behaviour-preserving change and verify with formatter/tests.

## Smell Documents

- `Long Function`
  - File: `agent/refactors/code_smells/bloaters/LONG_FUNCTION.md`
  - Use when a function mixes distinct step groups and is hard to reason about as one unit.
  - Common starter techniques:
    - `Extract Function` (`agent/refactors/techniques/composing_functions/EXTRACT_FUNCTION.md`)
    - `Replace Temp with Query` (`agent/refactors/techniques/composing_functions/REPLACE_TEMP_WITH_QUERY.md`)
    - `Decompose Conditional` (`agent/refactors/techniques/simplifying_conditional_expressions/DECOMPOSE_CONDITIONAL.md`)

## Smell-to-Technique Starter Mapping

- If the smell is mostly mixed responsibilities in one clause:
  - Start with `Extract Function`.
- If the smell is mostly nested branching:
  - Start with `Replace Nested Conditional with Guard Clauses`.
- If the smell is mostly parameter clutter during extraction:
  - Use `Introduce Parameter Struct` or `Preserve Whole Struct`.

## Terminology Crosswalk (Legacy -> Current Name)

Some smell docs and references use classic Fowler names. Use these repository names:

- `Introduce Parameter Object` -> `Introduce Parameter Struct` (`agent/refactors/techniques/simplifying_function_calls/INTRODUCE_PARAMETER_STRUCT.md`)
- `Preserve Whole Object` -> `Preserve Whole Struct` (`agent/refactors/techniques/simplifying_function_calls/PRESERVE_WHOLE_STRUCT.md`)
- `Replace Method with Method Object` -> `Replace Function with Module` (`agent/refactors/techniques/composing_functions/REPLACE_FUNCTION_WITH_MODULE.md`)
- `Move Method` -> `Move Function` (`agent/refactors/techniques/moving_features_between_modules/MOVE_FUNCTION.md`)
- `Form Template Method` -> `Form Template Function` (`agent/refactors/techniques/dealing_with_generalization/FORM_TEMPLATE_FUNCTION.md`)
- `Parameterize Method` -> `Parameterize Function` (`agent/refactors/techniques/simplifying_function_calls/PARAMETERIZE_FUNCTION.md`)
