# Skills Directory

This file is the guide for the `.agent/skills/` directory. Treat the reader as a complete beginner to this repository: they have only the current working tree, this guide, and the skill files stored under `.agent/skills/`. There is no memory of prior skill discussions and no outside context.

Use this guide to decide what belongs in `.agent/skills/`, when to add a new skill instead of revising an existing one, where each file should live, and what every `SKILL.md` file must contain at minimum before it is ready to use.

## Definitions

- `Skill`: A reusable instruction file that tells a reader how to perform one class of task.
- `Trigger`: The observable condition that tells the reader the skill should be used.
- `Constraint`: A rule the reader must not break.
- `Validation`: The observable proof that the skill was followed correctly and produced the required result.
- `Template`: A reusable fill-in-the-blank structure that keeps skills consistent.
- `Asset`: A supporting file or folder stored beside a skill, such as an example, template, reference snippet, or script.

## Purpose / Big Picture

The `.agent/skills/` directory stores reusable skills. A skill is a repeatable instruction file for one class of task. The directory exists so humans and coding agents can carry out recurring work without relying on memory, hidden team context, or one-off prompts.

This guide has two jobs. First, it explains how to work inside `.agent/skills/` as a local directory: how the tree is organized, how to add or revise a skill, and how to keep supporting files in the correct place. Second, it defines the minimum contract that every `SKILL.md` file in this directory must satisfy, plus a recommended base layout authors may follow.

## Output

- Primary artifact: A new or revised skill stored under `.agent/skills/<skill_name>/SKILL.md`, with any supporting files kept inside the owning skill directory under clearly named subdirectories when needed.
- Primary consumer: A coding agent or human novice who needs to add, revise, or use a skill from this directory without guessing.
- Ready when: The directory layout is correct, the skill boundary is clear, the skill package is self-contained, and any needed supporting files are stored and named clearly enough for a beginner to find.
- Hands off to: Use `Start Here` and `When to Create, Extend, or Stop` to decide whether the work belongs in this directory, another skill directory, a root `.agent` guide, or a one-off prompt.

## When to use this document

Use this document when you need to:

- create a new skill
- rewrite an unstructured prompt into a reusable skill
- standardize several skills so they all satisfy the same minimum contract
- make a skill easier for a novice or coding agent to follow

## When not to use this document

Do not use this document when:

- you only need a one-off prompt for a single task
- the instructions are temporary and will not be reused
- the task is too small to justify a full skill file

## How to Use this document

Start here any time you touch `.agent/skills/`. Keep this file open while you work on a skill. If the local directory layout, naming convention, minimum required skill contract, or support-file placement rules change, update this guide in the same change so the next reader can restart from the working tree alone.

Read `Start Here` when you want the fastest answer. Read `Directory Layout` and `Important Path Rules` before you create or move files. Read `Writing a Skill in This Directory` when you need the minimum required sections, compatibility rules, and recommended authoring pattern.

## Guidelines

- Create a new reusable skill. Add a new directory under `.agent/skills/<skill_name>/`, create `SKILL.md`, and follow `Writing a Skill in This Directory`.

- Revise an existing skill. Update the existing `.agent/skills/<skill_name>/SKILL.md` and keep any skill-owned supporting files in sync.

- Add supporting examples, references, scripts, or templates. Keep them inside `.agent/skills/<skill_name>/` under clearly named subdirectories and update the skill so it points to them explicitly.

- Unsure whether this should be a skill at all. Read `When to Create, Extend, or Stop` before you add anything.

- Need a root planning or policy document instead of a reusable skill. Stop here and use the relevant root `.agent` guide instead.

## Important Path Rules

Use these path rules as the source of truth for this directory.

- Every skill lives in its own directory under `.agent/skills/<skill_name>/`.
- The instruction file inside a skill directory is always named `SKILL.md`.
- Supporting files stay inside `.agent/skills/<skill_name>/` under clearly named subdirectories such as `assets/`, `references/`, `scripts/`, or other skill-owned folders that describe their purpose.
- Keep skill directory names short, lowercase, and specific to one task or one tightly related skill area.
- Do not place free-floating skill files or shared top-level support folders directly under `.agent/skills/` other than this `AGENTS.md` directory guide.

## Directory Layout

Use this section as the local source of truth for what is checked into `.agent/skills/` today.

```text
.agent/skills/
├── AGENTS.md
└── documentation/
    ├── SKILL.md
    └── assets/
        └── examples/
            ├── changeset.md
            ├── channel.md
            ├── code_reloader.md
            ├── ecto.md
            ├── logger.md
            ├── query.md
            ├── queryable.md
            ├── repo.md
            ├── socket.md
            ├── storage.md
            └── transaction.md
```

- `.agent/skills/AGENTS.md`: The directory guide you are reading now. It explains how to work in `.agent/skills/` and the minimum contract every `SKILL.md` must satisfy.
- `.agent/skills/documentation/`: A skill directory reserved for documentation-related skill content.
- `.agent/skills/documentation/SKILL.md`: The local instruction file path for the documentation skill.
- `.agent/skills/documentation/assets/`: Supporting files for the documentation skill area.
- `.agent/skills/documentation/assets/examples/`: Example markdown fragments used by the documentation skill.
- Example files under `.agent/skills/documentation/assets/examples/`: Checked-in supporting examples for documentation-related work. These are assets, not standalone skills.

## When to Create, Extend, or Stop

Create a new skill when the task repeats, the trigger conditions are observable, and one self-contained procedure should be followed each time.

Extend an existing skill when the new work is still the same task, uses the same trigger, and belongs in the same skill boundary. If the new material adds a second unrelated task or makes the trigger ambiguous, create a new skill instead.

Stop and use another root `.agent` guide when the real job is to write an investigation, behaviour specification, execution plan, refactor plan, ADR, architecture review, or style rule. A skill may support those workflows, but it should not replace the document that owns them.

Use a one-off prompt instead of a skill when the instructions are temporary, highly specific to one task, or too small to justify a maintained directory entry under `.agent/skills/`.

## Requirements

NON-NEGOTIABLE REQUIREMENTS:

* Every skill stored in `.agent/skills/` must live in its own directory and must use `SKILL.md` as the instruction file name.
* Every skill package must be self-contained, meaning that `SKILL.md` together with any skill-owned support files it points to gives a novice enough information to succeed without hidden context.
* Every skill must cover one kind of task, not several unrelated task types.
* Every skill body must include all of these sections somewhere: `Purpose`, `Goal`, `When to use`, `When not to use`, `Inputs`, `Outputs`, `What to do`, `Decision rules`, `Constraints`, `Validation`, `Success criteria`, `Examples`, and `Common mistakes`.
* Optional frontmatter or metadata above the skill body is allowed when another compatible skill workflow or tool expects it.
* Extra sections are allowed before, between, or after the required sections as long as they do not replace or contradict the minimum contract.
* Every skill must define any term of art before it relies on that term.
* Every skill must make its responsibility boundaries explicit.
* Every skill must be specific enough that two different readers would take the same action.
* Every skill-owned supporting file must belong to a specific skill, live inside that skill's directory, and be named clearly enough that a beginner can tell why it exists.

Treat these rules as mandatory. If one is missing, the skill is incomplete and is not safe for a beginner to use.

## Working in This Directory

### Adding a New Skill Directory

Create a new directory under `.agent/skills/<skill_name>/` when the task does not fit an existing skill boundary. Add `SKILL.md` inside that directory first. Add supporting subdirectories only when the skill genuinely needs them, such as examples, references, scripts, templates, or other skill-owned material.

Name the directory for one task or one tightly related skill area. Prefer a short lowercase name that will still make sense when read in the tree by itself.

### Revising an Existing Skill

Revise an existing skill when the trigger, task boundary, and overall purpose are still the same. Keep the skill package self-contained as you revise it. If you change the minimum contract, naming convention, or support-file placement rules for the directory as a whole, update this guide in the same change.

If the revision would cause the skill to own multiple unrelated tasks, split the work instead of widening the skill until its trigger is vague.

### Adding or Reorganizing Supporting Files

Use clearly named subdirectories inside the owning skill directory for supporting material that the skill refers to directly. Good examples include `assets/`, `references/`, `scripts/`, or metadata folders when they keep the main `SKILL.md` concise and easier to use.

Keep support files beside the skill that owns them. Do not create shared top-level support folders under `.agent/skills/`. If you move, rename, add, or remove support files, update the owning `SKILL.md` so the file paths still match the working tree.

### Keeping This Document in Sync

Update `Directory Layout` whenever the checked-in `.agent/skills/` tree changes. Update `Important Path Rules`, `Writing a Skill in This Directory`, the skeleton, and the example whenever the minimum contract, compatibility rules, or recommended layout changes.

If this guide describes the current contents of a checked-in skill directory, keep those descriptions accurate.

## Writing a Skill in This Directory

Use this section when you are authoring or revising `.agent/skills/<skill_name>/SKILL.md`.

### Minimum Required Skill Contract

Every skill body must include these sections. They do not need to be the only sections, and they do not need to appear in this exact order.

- `Purpose`: Explain what the skill is for, what problem it solves, and why it exists.
- `Goal`: State the end result the skill should produce, not the process.
- `When to use`: Describe the exact situations that should trigger the skill. Use observable conditions.
- `When not to use`: Describe when the skill is the wrong choice. This prevents false triggers and overlap.
- `Inputs`: Name the information the skill needs before it can succeed.
- `Outputs`: Name what the skill must produce.
- `What to do`: Write the exact procedure to follow as ordered actions.
- `Decision rules`: Explain how to choose between paths when the skill allows more than one action.
- `Constraints`: List the rules the reader must not break.
- `Validation`: Explain how to prove the skill was applied correctly.
- `Success criteria`: State what must be true before the task is complete.
- `Examples`: Show at least one realistic example that removes ambiguity.
- `Common mistakes`: List the mistakes that cause the skill to fail or drift.

### Allowed Extensions

The minimum contract above is authoritative, but it is not the only allowed structure.

- Optional frontmatter or metadata above the body is allowed when another skill-creation workflow, tool, or consumer expects it.
- Extra sections such as `Glossary`, `Background`, `References`, `Quick start`, or `Troubleshooting` are allowed before, between, or after the required sections.
- Skill-owned support folders such as `assets/`, `references/`, `scripts/`, or metadata directories are allowed when the skill points to them clearly and they remain inside the owning skill directory.

### Recommended Base Layout

If you do not have a good reason to do otherwise, use the required sections in the following order. Treat this as the default template, not as an exclusive format.

1. `Purpose`
2. `Goal`
3. `When to use`
4. `When not to use`
5. `Inputs`
6. `Outputs`
7. `What to do`
8. `Decision rules`
9. `Constraints`
10. `Validation`
11. `Success criteria`
12. `Examples`
13. `Common mistakes`

### Recommended Authoring Workflow

1. Name the skill so the title describes one kind of task, not a vague area of help.
2. Write `Purpose` and `Goal` so a beginner knows what the skill is trying to achieve.
3. Write `When to use` and `When not to use` so the trigger boundary is observable.
4. Name the `Inputs` and `Outputs` explicitly.
5. Write `What to do` as ordered actions with no hidden assumptions.
6. Add `Decision rules` and `Constraints` so the reader knows how to choose and what they must not do.
7. Add `Validation`, `Success criteria`, `Examples`, and `Common mistakes` so the skill proves its own result.
8. Read the whole skill as a novice and remove any wording that requires background knowledge or guessing.

### Writing Rules

Write in plain language. Prefer short sentences, familiar words, and one idea per sentence.

Use observable language. Prefer instructions that can be checked from the outside, such as `Run mix test test/example_test.exs.` or `Include the required sections such as Purpose, When to use, and What to do.` Avoid vague language such as `Handle this properly.` or `Do the normal setup.`

Use imperative steps inside `What to do`. Prefer `Define the trigger conditions.` or `List the required inputs.` Avoid status-style instructions such as `The trigger conditions are defined.`

Define terms of art in plain language or remove them. If a beginner would not know the word, the skill must explain it before relying on it.

Prefer exact boundaries. State clearly what is included, what is excluded, and where the skill stops owning the task.

### Validation, Evidence, and Examples

Validation should prove the output of the skill, not merely restate the intention. Use observable evidence such as commands, file paths, output shape, or required sections present in the result.

A worked example should include the trigger, the relevant input, the action the skill takes, and the expected output shape. A good example removes ambiguity that the section descriptions alone cannot remove.

If a skill depends on reusable examples, templates, or support files, keep them inside the owning skill directory under clearly named subdirectories and make sure the skill names them precisely enough that a beginner can find them from the working tree alone.

The skill is complete only when all of the following are true:

- The skill has a clear name that matches one kind of task.
- The skill body contains every required section from `Minimum Required Skill Contract`.
- The skill explains when it should be used.
- The skill explains when it should not be used.
- The skill tells the reader exactly what to do.
- The skill defines the required inputs.
- The skill defines the expected outputs.
- The skill lists the rules and limits that must be followed.
- The skill explains how to verify that the result is correct.
- The skill includes at least one concrete example.
- A novice can read the skill and use it without outside explanation.

## Recommended Skill Skeleton

Use this skeleton when you create a new `SKILL.md` and do not need a more specialized layout. It shows the recommended base layout, not the only allowed format.

Optional frontmatter or metadata may appear above this skeleton. Additional sections may also appear before, between, or after the required sections when they help the skill.

    # <Skill name>

    ## Purpose
    <Explain what this skill is for and why it exists.>

    ## Goal
    <State the outcome this skill should produce.>

    ## When to use
    - <Observable trigger condition>
    - <Observable trigger condition>

    ## When not to use
    - <Clear exclusion>
    - <Clear exclusion>

    ## Inputs
    - <Required input>
    - <Required input>

    ## Outputs
    - <Expected output>
    - <Expected output>

    ## What to do
    1. <Action step>
    2. <Action step>
    3. <Action step>

    ## Decision rules
    - <Rule for choosing between paths>
    - <Rule for handling ambiguity>

    ## Constraints
    - <Rule that must not be broken>
    - <Rule that must not be broken>

    ## Validation
    - <Observable proof that the skill was applied correctly>
    - <Observable proof that the output is correct>

    ## Success criteria
    - <Binary completion condition>
    - <Binary completion condition>

    ## Examples

    ### Example 1

    **Input:** <Example input>

    **Action:** <What the skill does>

    **Output:** <Expected result>

    ## Common mistakes
    - <Common mistake>
    - <Common mistake>

## Example Skill

Use this example as a model for scope, specificity, and tone. It demonstrates the minimum required sections arranged in the recommended base layout. Real skills may also include optional frontmatter, extra sections, or skill-owned support directories when those additions improve compatibility or clarity.

    # Rewrite existing documentation

    ## Purpose
    Use this skill to rewrite existing documentation so it is easier to read, clearer for beginners, and more consistent in structure.

    ## Goal
    Produce revised documentation that preserves the original meaning while improving clarity and readability.

    ## When to use
    - The user asks to rewrite, polish, simplify, or clarify existing documentation.
    - The input already contains source text that needs improvement.

    ## When not to use
    - The user asks for net-new documentation from scratch.
    - The task requires technical validation rather than writing improvement.

    ## Inputs
    - Source text to revise.
    - Required tone, format, or documentation style.

    ## Outputs
    - Revised documentation.
    - Preserved meaning with improved structure and wording.

    ## What to do
    1. Read the full source text before rewriting any part.
    2. Identify the purpose of the text and the intended reader.
    3. Preserve the meaning of the original text.
    4. Rewrite the text using shorter sentences and clearer wording.
    5. Remove ambiguity and define terms that a beginner may not know.
    6. Preserve any required structure, headings, or constraints from the task.

    ## Decision rules
    - If clarity and literal phrasing conflict, preserve meaning first and then improve wording.
    - If a term of art is required, define it in plain language.

    ## Constraints
    - Do not change the intended meaning.
    - Do not remove required details.
    - Do not introduce new requirements that were not in the source text.

    ## Validation
    - Compare the revised text to the original and confirm that the meaning did not change.
    - Confirm that the revised text is easier to scan and uses clearer wording.

    ## Success criteria
    - The meaning matches the source text.
    - The revised text is clearer than the original.

    ## Examples

    ### Example 1

    **Input:** A paragraph with long sentences and unclear wording.

    **Action:** Rewrite the paragraph in plain language while keeping the same meaning.

    **Output:** A shorter, clearer paragraph that preserves the original intent.

    ## Common mistakes
    - Rewriting the text into a different meaning.
    - Removing important constraints while simplifying the wording.

## Contributor Maintenance Rules

This file is only useful if it stays aligned with the actual `.agent/skills/` tree and the minimum contract it prescribes. Update it in the same change whenever the skills directory changes.

- If a skill directory is added, removed, renamed, or moved, update `Directory Layout`.
- If the minimum contract, compatibility rules, or recommended layout changes, update `Writing a Skill in This Directory`, the skeleton, and the example in the same change.
- If supporting files move or change shape, update this guide and the owning `SKILL.md` so the written instructions still match the working tree.
- If this guide makes claims about the current contents of a checked-in skill directory, keep those claims accurate.
- If you discover a repeated task that deserves a reusable skill, add the skill instead of relying on memory or prior discussion.
