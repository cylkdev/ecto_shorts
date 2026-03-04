# Windsurf workflow: create a brand new Elixir application

You are creating a new Elixir project. Elixir projects are created with a tool called mix. mix is included when you install Elixir.

At the end, you will have:

- A project folder on disk
- A file called `mix.exs` at the project root (this defines your project)
- A working test suite you can run
- A working format command you can run

## Before you start

Before you start you must verify all of the following:

1. Elixir is installed (and mix is available).
2. You know what folder the project should live in.

Run the following command to verify Elixir is installed:

    elixir -v

Run the following command to verify mix is installed:

    mix -v

If either command fails, try to install Elixir using `asdf` before you proceed. If Elixir cannot be installed then exit the workflow and stop.

## How to install Elixir using asdf

## How to use Windsurf for this workflow

Windsurf usually gives you:

- A code editor (to create and edit files)
- A terminal (to run commands)
- A chat/agent panel (to ask the agent to do steps or check your work)

This workflow assumes you will use the terminal for commands, and use the agent to explain errors and review files.

When you copy commands from this workflow, run them exactly as written.

## Step 1: choose a project name

Pick a short name using lowercase letters and underscores.

Good examples:

- `hello_world`
- `blog_app`
- `todo`

Avoid spaces and dashes.

In Elixir, your application also gets a module name version of this. For example, `blog_app` becomes `BlogApp`.

## Step 2: create the project

In a terminal, go to the folder where you want the project to live.

Then run:

    mix new blog_app

Replace `blog_app` with your project name.

This creates a new folder with starter code.

Now enter the project folder:

    cd blog_app

## Step 3: understand the important files (so you don't feel lost)

You do not need to memorize this, but it helps to know what is what.

- `mix.exs` is the project definition. It lists your app name, version, dependencies, and build settings.
- `lib/` is where your application code lives.
- `test/` is where your tests live.
- `test/test_helper.exs` sets up the test environment.
- `.formatter.exs` controls formatting rules.

## Step 4: make sure it builds

Run:

    mix compile

If compilation succeeds, you are in a good place.

If it fails, copy the full error output into the Windsurf agent and ask it to explain what to do next. Always include the command you ran and the full output.

## Step 5: run the tests

Run:

    mix test

A new project should have at least one simple test. This confirms your setup is working.

## Step 6: run the formatter

Run:

    mix format

This formats your code to Elixir's standard style.

If you are working with an agent, make it a habit to run `mix format` before you commit changes.

## Step 7: run the app in interactive mode (optional but useful)

Elixir has an interactive shell called IEx. You can start it with your project loaded:

    iex -S mix

Now you can call functions from your project.

To exit, press `Ctrl+C` twice.

## Step 8: create your first module (a simple "it works" feature)

Create a new file at:

    lib/blog_app/greeting.ex

Replace `blog_app` with your project name.

Put this code in the file. Make sure the module name matches your project name:

    defmodule BlogApp.Greeting do
      def hello(name) when is_binary(name) do
        "Hello, " <> name <> "!"
      end
    end

If your project is named `todo`, this module would be `Todo.Greeting`.

Now create a test file:

    test/blog_app/greeting_test.exs

Add this code (again, match the module name to your project):

    defmodule BlogApp.GreetingTest do
      use ExUnit.Case, async: true

      test "hello/1 returns a greeting" do
        assert BlogApp.Greeting.hello("Sam") == "Hello, Sam!"
      end
    end

Run the tests again:

    mix test

If it passes, you successfully added real code and validated it.

## Step 9: basic habits that prevent pain later

Always do these in this order when you change code:

    mix format
    mix compile
    mix test

If something fails, do not ignore it. Fix it before moving on.

## Step 10: add Git version control (recommended)

From the project root, run:

    git init
    git add .
    git commit -m "Initial commit"

This gives you a clean starting point.

## Troubleshooting rules that work for beginners

If you hit an error:

1. Re-run the command once to make sure it is consistent.
2. Read the first and last lines of the error output. The first line usually says what failed. The last lines usually show the file and line number.
3. If you are stuck, paste the full terminal output into the Windsurf agent.

When you ask an agent for help, include:

- Your operating system (macOS, Windows, Linux)
- The exact command you ran
- The entire output
- The project name you used

That information is usually enough to get a correct fix on the first try.

## "Done" checklist

You are done when all of these work from the project folder:

    mix format
    mix compile
    mix test
    iex -S mix

When those work, you have a working Elixir application skeleton that you can build on.