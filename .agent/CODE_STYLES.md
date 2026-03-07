# Writing a Coding Style Reference Document (CodeStyleDocs)

This document explains how to write a coding style reference document that a coding agent or a human novice can follow to write a code style rule. Treat the reader as a complete beginner to this repository: they have only the current working tree and the single `CodeStyleDocs/` directory you provide. There is no memory of prior code style documents and no external context.

A human or coding agent must be able to produce a reference

The goal is to help them produce the same kind of reference document, in the same tone and shape, every time. This follows the same self-contained and novice-guiding approach used in the PLANS.md example, where the document must carry the full context needed to do the work.

A good coding style reference document is not just a note about preference. It explains a problem in code, shows a concrete example of the problem, explains why the problem matters, and then shows a better way to write the code. The example rule you were given follows exactly that pattern: it has a title, a `Problem` section, an example of the anti-pattern, and a `Solution` section with improved code. It also explains the reasoning in plain language instead of assuming the reader already knows it.

## What this document is for

Use this document when you need to write a coding style rule that teaches one rule clearly and consistently. The output should help a reader answer these questions without guessing: what is the bad pattern, why is it bad, what should be written instead, and how can I recognize the difference in real code. The final rule should be short enough to read quickly, but complete enough that a beginner can still apply it correctly. This matches the same general principle from the PLANS.md example: explain the purpose first, then give the exact information needed to act. 

## What a coding style reference document must do

A coding style reference document must be self-contained. The reader should not need prior discussion, hidden context, or outside knowledge. If the rule uses a term that is not ordinary language, define it in the document or do not use it. The document must explain the rule in plain prose. It should prefer sentences over lists. It should only use lists when they make the document easier to follow. Those are the same core writing expectations the PLANS.md page gives for beginner-friendly technical documents.

A coding style reference document must also be concrete. It should not say vague things like “write cleaner code” or “prefer readability.” Instead, it should show the exact code shape that is discouraged and the exact code shape that is preferred. In the example rule, the anti-pattern is a `with` expression with a complex `else` block, and the preferred form moves error normalization into private helper functions. The document works because the reader can see both forms clearly. 

## The shape of the document

Follow one simple structure every time.

Start with a title that names the bad pattern or the rule. The title should be short and direct.

Then write a `Problem` section. This section explains what the anti-pattern is. It should say what the code is doing, why that shape is hard to read, hard to maintain, or easy to misunderstand, and what confusion it creates for the reader.

After that, write an `Example` section. Show a realistic code example that contains the bad pattern. The example must be small enough to read in one pass, but real enough that the problem is obvious.

Then write a short explanation under the example. This is where you connect the code to the problem in plain language. Explain exactly what is unclear, risky, or hard to follow about the example.

Then write a `Solution` section. This section explains the better approach. It should say what to do instead and why the new shape is easier to understand or maintain.

Finally, show a second code example that uses the preferred approach. The reader should be able to compare the two versions and understand the difference immediately. This is the same pattern used in the rule you provided.

## How to study the example rule before writing a new one

Before writing a new rule, read the example rule from top to bottom at least once without editing anything. Your first goal is to understand the document shape, not the specific rule. Look for the repeated pattern: name the problem, show the bad code, explain the pain, show the better code, explain the benefit. That repeated pattern is what you should reproduce. The PLANS.md example recommends this same habit at a larger scale: start from the skeleton and fill it in carefully after reading the source material closely. 

Then read the example rule a second time and pay attention to the tone. The tone should be calm, direct, and explanatory. It should not sound dramatic, clever, or abstract. It should not insult the bad code. It should explain the cost of the pattern in simple words. In the uploaded example, the wording is plain and specific. It says the code is harder to read and maintain because the error source becomes unclear. That is the tone to match.

## How to choose the rule you are writing about

A good style rule covers one narrow problem. Do not try to teach many rules in one document. Pick one pattern that can be named clearly and shown with one bad example and one good example.

A good rule usually has these qualities. The bad pattern appears often enough to matter. The better pattern is clear and teachable. The difference between the two can be shown with a small code sample. The benefit is understandable in human terms such as readability, maintenance, consistency, or safer behaviour.

Avoid topics that are too broad. “Write good functions” is too broad. “Avoid complex else clauses in with” is narrow enough. The narrower the rule, the more consistent the output will be.

## How to write the `Problem` section

The `Problem` section should start by naming the anti-pattern in plain language. Then explain what the code shape looks like. Then explain why it causes trouble.

Keep the explanation tied to what a reader can observe in the code. Do not rely on taste. Do not say “this looks ugly.” Say what becomes harder. For example, maybe control flow becomes unclear, maybe responsibilities are mixed together, maybe error handling is too far away from the place where the error happens, or maybe repeated branching hides the main path.

A beginner should be able to read the `Problem` section and then recognize the bad pattern in a codebase.

## How to write the bad example

The bad example should show the problem clearly. Do not add extra logic that distracts from the rule. The example should be realistic, but focused.

Name the function and variables so the code is easy to follow. Keep the example long enough to make the problem visible, but short enough that the reader can compare it with the solution without effort.

The bad example must actually demonstrate the rule. Do not include an example that is only slightly imperfect. The problem should be obvious once the reader has read the explanation.

## How to explain the bad example

After the bad example, write one short paragraph that points directly at the problem. Use the actual shape of the code in your explanation.

For example, if the rule is about branching, explain how the branching breaks the reader’s ability to track the main path. If the rule is about mixed responsibilities, explain which responsibilities are being mixed. If the rule is about distant error handling, explain how the reader cannot easily tell which failure came from which step. This is what the uploaded example does when it explains that the `else` patterns are hard to connect back to the original `<-` clauses.

## How to write the `Solution` section

The `Solution` section should not only say what to change. It should also explain why the new shape is better.

Start by stating the new approach in one sentence. Then explain how that approach improves the code. The improvement should be framed in plain terms. For example, the main path becomes easier to read, errors are handled closer to where they happen, or each function now has one clearer responsibility.

The solution should feel like a direct answer to the problem you already described. The reader should be able to see that the new form removes the exact confusion caused by the old form.

## How to write the good example

The good example should keep the same basic behaviour as the bad example. Only change what is necessary to apply the rule. This makes the comparison fair and easy to follow.

When possible, preserve the same function name and similar variable names. That helps the reader compare shapes instead of trying to understand two different problems at once.

The good example should make the benefit visible in the code itself. In the uploaded example, the main `with` now focuses on the success path, while the helper functions normalize errors near the source. The reader can see the improvement without extra explanation.

## Tone and dialect

Write in plain language. Use simple words. Prefer short sentences. Explain cause and effect directly.

Do not write like a textbook. Do not write like marketing. Do not write like a linter error message. Write like a careful engineer teaching a beginner.

Use statements that have one clear meaning. Avoid vague words like “better” unless you immediately explain what makes it better. Avoid jokes, sarcasm, and filler.

The PLANS.md example is useful here because it consistently uses direct instructional prose, defines the task clearly, and avoids hidden assumptions. That is the same writing discipline you should use for a style reference document, even though the document is much smaller.

## What to avoid

Do not write a rule that depends on unwritten project history. The reader may not know why the team prefers one pattern unless you explain it.

Do not write a rule that only says “do this” and “do not do this” without showing code. This makes the rule hard to apply consistently.

Do not overload the document with many side cases. If the rule has many exceptions, the rule is probably too large. Narrow the scope.

Do not use undefined jargon. If you say words like “normalize,” “control flow,” or “branching,” make sure the meaning is clear from the sentence.

Do not explain two different problems in one rule. One document should teach one rule.

## A repeatable process for writing the rule

First, identify the one code pattern the rule is about. State it in one sentence.

Next, write down what goes wrong when that pattern is used. Write this in human terms such as confusion, duplication, hidden behaviour, or mixed responsibility.

Then create a small bad example that shows the problem clearly.

After that, write one paragraph that explains the problem using the code you just showed.

Then decide on the better pattern. Write one sentence that says what to do instead.

Next, create a good example that keeps the same behaviour but uses the better pattern.

Finally, read the whole document once as if you are new to the codebase. Make sure every term is clear, every paragraph serves one purpose, and the good example really solves the problem shown by the bad example.

## Validating a Code Style Reference Document

A code style reference document is valid only when a complete beginner can read it with only the document and the current working tree, and then reach the same interpretation you did.

The reader must not need extra explanation from you. The reader must not need hidden project history. The result must be visible in what they say, what they point to, and what they write. The reader must not need additional context.

Use this checklist to validate the document.

  - [ ] The reader agrees with your interpretation of what pattern the rule does not allow.
  - [ ] The reader agrees with your interpretation of why the pattern is a problem.
  - [ ] The reader agrees with your interpretation of what part of the bad example violates the rule.
  - [ ] The reader agrees with your interpretation of what change makes the good example correct.
  - [ ] The reader is able to write a new reference document in the same format from a different example in the working tree.

The document passes only when all of these are true.

A result does not count if the reader only gives a vague answer. A result does not count if the reader needs you to explain what the document meant. A result does not count if the reader reaches the right answer only after back and forth discussion. The document must produce the same understanding on its own.

To make this check observable, require the reader to produce written output that demonstrates their understanding of the rule.

## Written Artifacts

A written artifact is required for each result.

A written artifact is:

- A short written statement that names the forbidden pattern.
- A short written statement that explains the problem the pattern causes.
- An annotated bad example that marks the exact offending code.
- An annotated good example that marks the exact improvement.
- A new reference document that matches the required sections and format.

This makes the validation deterministic because every check is based on something a reviewer can see, compare, and verify.

## Skeleton of a Good Code Style Reference Document

    Use this skeleton when writing a new coding style reference document.

    # <Short action-oriented rule name>

    ## Problem

    This anti-pattern refers to <plain description of the bad pattern>.

    This pattern is harmful because <plain explanation of what becomes harder, less clear, or less safe>.

    ### Example

    <bad example code>

    In the code above, <plain explanation of what is hard to understand, easy to misread, or difficult to maintain>.

    ### Solution

    In this situation, instead of <bad approach>, it is better to <preferred approach>.

    This makes the code easier to <plain benefit>. It also helps because <second plain benefit if needed>.

    <good example code>

    ## Writing notes

    Keep the title short and direct.

    Use plain prose.

    Prefer sentences over lists.

    Show one bad example and one good example.

    Explain the problem and the solution in simple words.

    Keep the examples focused on one rule.

    Do not assume prior context.

    Define uncommon terms or do not use them.