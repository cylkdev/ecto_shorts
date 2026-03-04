---
trigger: always_on
---

## Requirements

NON-NEGOTIABLE REQUIREMENTS:
* Any change to these files must go through the User Approval Workflow, because they control project-wide rules and can affect the whole codebase. These files are protected
which means they cannot be changed in any way shape or form without the user being aware of the proposed changes.

    - `.windsurf/rules/project-guidelines.md`
    - `.credo.exs`
    - `mix.exs`

Changes being made to application code are not required to use the User Approval Workflow unless it was explicitly requested by the user.

* You must follow the User Approval Workflow steps before editing any of the protected files.

* Your explanation must be easy to understand at a glance, easy to review, and easy to verify with a repeatable check.

* Treat the reader as a complete beginner to this repository: they have only the current working tree and the single ExecPlan file you provide. There is no memory of prior plans and no external context.

* Your explanations must be fully self-contained. Self-contained means that in its current form it contains all knowledge and instructions needed for a human novice to succeed.

## User Approval Workflow

Do not skip any steps of the User Approval Workflow.

1. Start by naming the exact file you want to change and describing the smallest possible edit you intend to apply. Your description must be specific enough that someone could predict what will happen after the change without guessing.

2. Next, explain the user-visible impact in plain language. User-visible means a developer can observe it directly. Examples include that a lint warning starts appearing, a lint warning stops appearing, mix test begins failing, mix test starts passing, a dependency version changes, or a new build step is required.

3. Then, show the proposed change as a patch. A patch is the most readable way to show what will change because it makes the before and after visible in one view.

4. After that, explain how to verify the change using a repeatable command. The verification must be something a human novice can run and understand. If the change affects linting, the verification should include running mix credo. If the change affects formatting rules, the verification should include running mix format. If the change affects dependencies, the verification should include running mix deps.get and then compiling or running tests. If you are not sure which command proves the change, you must choose the simplest command that demonstrates the effect you described.

5. You must then get explicit user approval for the patch before applying it. Approval means the user clearly agrees to the exact change you showed, not a general "sounds good."

6. Once approval is given, apply the change and immediately run the verification step you described. You must report the observable result in plain language, including whether anything unexpected happened.