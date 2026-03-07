# Avoid Application.put_env in Tests

Using `Application.put_env/4` (or `Application.delete_env/3`) inside a test is typically a code smell. It changes global state for the entire VM. That means one test can accidentally affect another test, especially when tests run at the same time.

This often shows up when code reads config directly at runtime, like `Application.get_env/3`, and the test tries to "patch" config to force a behaviour.

This is bad because:

- It makes tests order-dependent. If one test forgets to restore the old value, later tests can fail in confusing ways.
- It breaks async safety. Two tests can race and overwrite each other's config.
- It hides the real dependency. The code depends on config, but the dependency is not visible in the function signature.

Instead you should do one of the following:

1. Pass the value as a function argument (best default).
2. Pass a module (behaviour) as a dependency (good when the dependency is "how to do something", not just a value).
3. Compute config once at startup and store it in process state (good for OTP processes, avoids repeated global reads).

## Example: The Problem

This code reads config directly and forces the test to mutate global env.

    defmodule MyApp.TokenSigner do
      def sign(payload) do
        secret = Application.get_env(:my_app, :signing_secret)
        MyApp.Crypto.hmac(payload, secret)
      end
    end

A test might do this:

    defmodule MyApp.TokenSignerTest do
      use ExUnit.Case, async: true

      test "signs with the configured secret" do
        Application.put_env(:my_app, :signing_secret, "test-secret")

        assert MyApp.TokenSigner.sign("abc") ==
                 MyApp.Crypto.hmac("abc", "test-secret")
      end
    end

That test is not safe to run async, and it can leak config into other tests.

## Example: The Solution

Make the dependency explicit by accepting it as an option, and keep a small default for production callers.

    defmodule MyApp.TokenSigner do
      @default_app :my_app

      def sign(payload, opts \\ []) do
        secret = Keyword.get_lazy(opts, :secret, fn -> signing_secret() end)
        MyApp.Crypto.hmac(payload, secret)
      end

      defp signing_secret do
        Application.fetch_env!(@default_app, :signing_secret)
      end
    end

Now the test never touches global config:

    defmodule MyApp.TokenSignerTest do
      use ExUnit.Case, async: true

      test "signs with the provided secret" do
        assert MyApp.TokenSigner.sign("abc", secret: "test-secret") ==
                 MyApp.Crypto.hmac("abc", "test-secret")
      end
    end
    
Using `Application.put_env/4` is only valid when it's to test a specific behaviour (for example, a test that checks your supervision tree or init logic reads config correctly). Keep those tests `async: false`, and restore state with `on_exit/1`.

For most feature and unit tests, treat "test changes global application env" as a smell and refactor toward explicit dependencies.
