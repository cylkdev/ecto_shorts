# Keep Vocabulary Consistent

## Input Parameters

Accept the following input parameters:

- `directory`: Required. This is the directory to scan for markdown files.

In the instructions below, any pattern written as `{<name>}` means the value provided for the input parameter with that name.

## What to do

Review only the `*.md` files in `{directory}`.

Check whether those files use the same vocabulary consistently.

Treat a vocabulary inconsistency as any case where two or more words or phrases are used to refer to the same concept.

Make a list of every vocabulary inconsistency you find.

For each item in the list, record all of the following:

- the concept
- the conflicting words or phrases
- the exact file path
- the exact heading, section, or line reference that shows the inconsistency
- a short explanation of why the wording is inconsistent

Then work through the list one item at a time.

For each item, reason through the inconsistency before proposing a fix.

For each item, propose exactly 2 solutions.

For each solution, record all of the following in the list:

- the word or phrase to standardize on
- the places that would need to change
- the benefits of that solution
- the drawbacks of that solution

Do not move to the next item until the current item is resolved with the user.

Treat each item as a collaborative decision.

When you ask the user to choose how to resolve an item, present multiple choice options only.

Do not ask open-ended questions.

For each item, present the choices in this form:

- `A`: adopt solution 1
- `B`: adopt solution 2
- `C`: keep the current wording for now and return to this item later

After the user chooses an option, record the decision in the list and then continue to the next unresolved item.