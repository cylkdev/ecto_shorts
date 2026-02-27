---
auto_execution_mode: 3
description: Use these workflow steps when using absinthe
---

## Requirements

NON-NEGOTIABLE REQUIREMENTS

- Read `.agent/ABSINTHE.md` and load it into your context. If it is already in your context read it again to refresh your memory.

## How to create a phoenix web application

Use the following command to create a phoenix web application within an umbrella project:

```sh
# run from the apps/ directory
mix phx.new.web some_web--module SomeWeb --app some_web
```

Use the following command to create a phoenix web application outside of an umbrella project:

```sh
mix phx.new some_web--module SomeWeb --app some_web
```

## How to create a UserSocket module

Use the following command to create a phoenix user socket. Run this command from web application directory:

```sh
mix phx.gen.socket User
```

## How to setup an Absinthe application

Use the following steps to setup an absinthe application: 

1. Create a phoenix web application if one does not exist.

2. Add the absinthe dependencies to the mix.exs file. If a dependency is already present, do not add it again.

3. Run the command `mix deps.get` to install the dependencies.

4. Create the directory layout:

    lib/<web_app_name>/
      schema.ex                          
      schema/
        queries/
        mutations/
        subscriptions
        types/
      resolvers/

5. Create a UserSocket module if one does not exist. Add authorization to the socket so that it's not anonymous. Add a placeholder implementation to return `{:ok, %{current_user: %{id: 1}}}` for authorization if the project does not have a designated authorization api.

6. Add each pre-resolution and post-resolution middleware you are given to the schema module (`lib/<web_app_name>/schema.ex`).

7. Add `GQLErrorMessage.Absinthe.Middleware` to the post-resolution middleware if it does not already exist.

8. Run the command `mix test` from the application directory to run the tests.

9. If there are any warnings or errors, fix them.