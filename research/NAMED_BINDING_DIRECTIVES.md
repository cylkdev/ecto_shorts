## Named Binding Directives

Assumed starting query unless otherwise stated: `from(p in EctoShorts.TestPost)`

- `:with_named_binding`: create named bindings keyed by the outer binding name
  - `:join`: join definition used to create the named binding
  - `:source`: source joined by the nested join definition

### Examples

    [with_named_binding: [author: [join: [association: [source: :author]]]]]
    [with_named_binding: [author: [join: [association: [source: :author]]], users_table: [join: [table: [source: "users", on: true]]]]]

### Rule Statements

**Rule Statement 1:**

**Given** filter params: `[with_named_binding: [author: [join: [association: [source: :author]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided named binding directives  
**And** it must check whether the query adds the `:author` named binding by joining the `:author` association  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author)`

test name: "Rule Statement 1: with_named_binding single named binding"

**Rule Statement 2:**

**Given** filter params: `[with_named_binding: [author: [join: [association: [source: :author]]], users_table: [join: [table: [source: "users", on: true]]]]]`  
**When** the filter params are converted into a query condition  
**Then** it must apply the provided named binding directives  
**And** it must check whether the query adds the `:author` and `:users_table` named bindings from the provided join definitions  
**And** it must preserve the provided inputs exactly without adding implicit conditions  
**And** the resulting expression is: `from(p in EctoShorts.TestPost, join: a in assoc(p, :author), as: :author, join: u in "users", as: :users_table, on: true)`

test name: "Rule Statement 2: with_named_binding multiple named bindings"
