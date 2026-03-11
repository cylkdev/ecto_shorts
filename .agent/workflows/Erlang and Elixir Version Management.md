## Erlang and Elixir Version Management

This document describes various commands a human or coding agent can use to manage Erlang and Elixir versions using asdf.

- To verify that asdf is installed, run:

    asdf --version

- To install ASDF, go to the [ASDF documentation](https://asdf-vm.com/guide/getting-started.html) and follow the installation instructions for the current operating system.

- To check if the erlang plugin is installed, run:

    asdf plugin list | grep -qx 'erlang'

- To install the asdf erlang plugin, run:

    asdf plugin add erlang https://github.com/asdf-vm/asdf-erlang.git

- To check if the elixir plugin is installed, run:

    asdf plugin list | grep -qx 'elixir'

- To install the asdf elixir plugin, run:

    asdf plugin add elixir https://github.com/asdf-vm/asdf-elixir.git

- To set the erlang version for a project, run:

    asdf set erlang "<erlang_version>"

- To set the elixir version for a project, run:

    asdf set elixir "<elixir_version>"

- Use a `.tool-versions` file at the root of a project to record the versions of Erlang and Elixir to use for that project.

For example:

    erlang <erlang_version>
    elixir <elixir_version>

- To check if a specific version of erlang is installed, run:

    asdf where erlang "<erlang_version>"

- To install a specific version of erlang, run:

    asdf install erlang "<erlang_version>"

- If Erlang fails to install due to an OpenSSL error, you can use the following command to specify the OpenSSL path during installation:

    export KERL_CONFIGURE_OPTIONS="--without-javac --with-ssl=$(brew --prefix openssl)"
    asdf where erlang "<erlang_version>" >/dev/null 2>&1 || asdf install erlang "<erlang_version>"
    unset KERL_CONFIGURE_OPTIONS

- To check if a version of elixir is installed, run:

    asdf where elixir "<elixir_version>"

- To install a version of elixir, run:

    asdf install elixir "<elixir_version>"

- To install Hex, run:

    mix local.hex --if-missing --force

- To install Rebar, run:

    mix local.rebar --if-missing --force

- To verify the versions currently installed by asdf, run:

    asdf current

It should show the Erlang and Elixir versions you chose.

- To verify the Erlang version, run:

    erl -version

It should print the Erlang version.

- To verify the Elixir version, run:

    elixir --version

It should print the Elixir version.

- To verify the Mix version, run:

    mix --version

It should print the Mix version if erlang and elixir are installed correctly.
