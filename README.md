![flowrs_logo](https://media.githubusercontent.com/media/jvanbuel/flowrs/main/image/README/1683789045509.png)

[![CI](https://github.com/jvanbuel/flowrs/actions/workflows/ci.yml/badge.svg)](https://github.com/jvanbuel/flowrs/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/flowrs-tui.svg)](https://crates.io/crates/flowrs-tui)
[![Downloads](https://img.shields.io/crates/d/flowrs-tui.svg)](https://crates.io/crates/flowrs-tui)
[![License](https://img.shields.io/crates/l/flowrs-tui.svg)](https://github.com/jvanbuel/flowrs/blob/main/LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.87.0-blue)](https://github.com/jvanbuel/flowrs)
[![Built With Ratatui](https://ratatui.rs/built-with-ratatui/badge.svg)](https://ratatui.rs/)

Flowrs is a TUI application for [Apache Airflow](https://airflow.apache.org/). It allows you to monitor, inspect and manage Airflow DAGs from the comforts of your terminal. It is build with the [ratatui](https://ratatui.rs/) library.

![flowrs demo](https://media.githubusercontent.com/media/jvanbuel/flowrs/main/vhs/flowrs.gif)

## Installation

You can install `flowrs` via Homebrew if you're on macOS / Linux / WSL2:

```
brew install flowrs
```

or with `uv` on macOS (Apple silicon and Intel) and Linux x86_64:

```bash
uv tool install flowrs
```

You can also download the binary directly from GitHub:

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/jvanbuel/flowrs/releases/latest/download/flowrs-tui-installer.sh | sh
```

Alternatively, you can build `flowrs` from source with `cargo`:

```bash
cargo install flowrs-tui --locked
```

## Usage

### Managed Airflow services

The easiest way to user `flowrs` is with a managed Airflow service. The currently supported managed services are:

- [x] Conveyor
- [x] Amazon Managed Workflows for Apache Airflow (MWAA)
- [x] Google Cloud Composer
- [x] Astronomer

To enable a managed service, run `flowrs config enable -m <service>`. This will add the configuration for the managed service to your configuration file, or prompt you for the necessary configuration details. On startup `flowrs` will then try to find and connect to all available managed service's Airflow instances.

Note that for Astronomer, you need to set the `ASTRO_API_TOKEN` environment variable with your Astronomer API token (Organization, Workspace or Deployment) to be able to connect to the service.

### Custom Airflow instances

If you're self-hosting an Airflow instance, or your favorite managed service is not yet supported, you can register an Airflow server instance with the `flowrs config add` command:

![flowrs config add demo](https://media.githubusercontent.com/media/jvanbuel/flowrs/main/vhs/add_config.gif)

This creates an entry in your configuration file at `$XDG_CONFIG_HOME/flowrs/config.toml` (following the XDG Base Directory Specification, which defaults to `~/.config/flowrs/config.toml`). For backwards compatibility, flowrs also reads from `~/.flowrs` if the XDG location doesn't exist. If you have multiple Airflow servers configured, you can easily switch between them in `flowrs` configuration screen.

Flowrs supports authenticating with HTTP Basic Auth or using bearer tokens. When selecting the bearer token option, you can either provide a static token or a command that generates a token.

### Task logs from Loki

With the Kubernetes executor, Airflow reads a try's log from the pod that ran it. Once the pod is gone (it finished, was evicted, or its spot node was reclaimed), Airflow can only report where it looked. If your cluster ships pod output to Loki, flowrs can read the log from there instead, through Grafana's datasource proxy. Add a `grafana` section to the server in your configuration file:

```toml
[[servers]]
name = "prod"
endpoint = "https://airflow.example.com"
version = "V3"

[servers.auth.Basic]
username = "airflow"
password = "airflow"

[servers.grafana]
url = "https://grafana.example.com"
loki_datasource_uid = "abcd1234"
# Basic auth, or `token = "$GRAFANA_TOKEN"` for a service-account token.
# Grafana credentials written as `$NAME` or `${NAME}` are read from the environment.
username = "$GRAFANA_USERNAME"
password = "$GRAFANA_PASSWORD"
# Optional:
exclude_containers = ["vault-agent-init"]      # default
exclude_lines = "Requirement already satisfied|pip install"
max_lines = 20000                               # default
```

When Airflow returns only its "Log message source details" (or fails), the logs panel shows the Loki log instead, labelled `Task N · Loki`, with the pod and node it ran on. Press `L` in the logs panel to read from Loki even when Airflow still has the log. Loki is searched by the `dag_id`, `task_id` and `try_number` pod labels within the try's start and end time (2 minutes before to 5 minutes after); the `run_id` label is not used, because Kubernetes rewrites it. If a finished try has no `Task finished` event, flowrs points out that the pod stopped mid-run.

### Themes

Flowrs ships with six themes, including four [Catppuccin](https://github.com/catppuccin/catppuccin) flavors. The active theme is configured with `flowrs config --theme <theme>`:

| Theme | Description |
|---|---|
| `auto` | Detects your terminal background and picks dark or light (default) |
| `dark` | Dark theme |
| `light` | Light theme |
| `catppuccin-latte` | Catppuccin Latte (light) |
| `catppuccin-frappe` | Catppuccin Frappé (medium-dark) |
| `catppuccin-macchiato` | Catppuccin Macchiato (dark) |
| `catppuccin-mocha` | Catppuccin Mocha (darkest) |
