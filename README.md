<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/banner-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="./assets/banner-light.png">
    <img alt="Human-in-the-loop — Resonate" src="./assets/banner-dark.png">
  </picture>
</p>

<p align="center">
  <a href="https://resonatehq.github.io/examples-ci/">
    <img src="https://img.shields.io/endpoint?url=https://resonatehq.github.io/examples-ci/status/example-human-in-the-loop-go.json" alt="examples-ci status">
  </a>
</p>

# Human-in-the-Loop | Resonate Go SDK

A workflow that suspends on an external approval and resumes only when a human (or external system) resolves the latent durable promise.

> Heads up — `resonate-sdk-go` is pre-release. The SDK has no semver tag yet, so this example pins to a specific commit. Expect API changes until `v0.1.0`.

## What this demonstrates

- Using `ctx.Promise()` to create a **latent durable promise** — one with no registered function backing it. It resolves only when `promise.settle` is called externally.
- `Future.ID()` retrieves the promise ID your external system needs to resolve the workflow.
- `Future.Await(&out)` parks the workflow goroutine on the latent promise and decodes the settled value into `out`. The park survives worker crashes — if the process dies while waiting, another worker resumes the same workflow and waits on the same promise.
- Single-process simulation: a goroutine plays the role of the human reviewer, waiting ~3 seconds then calling `r.Sender().PromiseSettle(...)` to resolve the promise.

Use cases: approval gates, manual review queues, payment confirmation, legal sign-off — any step where a human action must precede workflow continuation.

## The code

```go
func approvalWorkflow(ctx *resonate.Context, req ReviewRequest) (string, error) {
    // Create a latent durable promise. No function is registered behind it.
    // The promise resolves only when promise.settle is called externally.
    f, err := ctx.Promise()
    if err != nil {
        return "", fmt.Errorf("ctx.Promise: %w", err)
    }

    // Hand the promise ID to whoever needs to resolve it —
    // an HTTP endpoint, a Slack bot, an email link, a CLI command, etc.
    promiseID := f.ID()
    fmt.Printf("waiting for approval — promise ID: %s\n", promiseID)

    // Park until the external actor resolves the promise.
    // The settlement value (e.g. "approved" or "rejected") is decoded here.
    var decision string
    if err := f.Await(&decision); err != nil {
        return "", fmt.Errorf("await approval: %w", err)
    }

    return fmt.Sprintf("completed with decision: %s", decision), nil
}
```

The simulator goroutine resolves the promise from outside the workflow:

```go
// Encode "approved" the same way the SDK's codec encodes values:
// JSON → base64 → quoted string, stored in Value.Data.
rawJSON, _ := json.Marshal("approved")
b64 := base64.StdEncoding.EncodeToString(rawJSON)
quotedB64, _ := json.Marshal(b64)
val := resonate.Value{Data: json.RawMessage(quotedB64)}

req := resonate.PromiseSettleReq{
    ID:    promiseID,
    State: resonate.SettleStateResolved,
    Value: val,
}
rec, err := r.Sender().PromiseSettle(ctx, req)
```

> **Note — resonate-sdk-go issue #28:** `PromiseSettle` is on the internal `Sender` type, reached via `r.Sender()`. There is no higher-level `Resonate.Promises` sub-client yet. In production you would wrap this in an HTTP handler or CLI command rather than calling it directly.
>
> **Don't reach for `resonate.NewValue` here** — there's a codec mismatch with `Future.Await` (raw JSON vs. base64-wrapped JSON). See [#22](https://github.com/resonatehq/resonate-sdk-go/issues/22).

## Production deployment pattern

In a real system you would replace the simulator goroutine with an external trigger:

**HTTP endpoint (e.g. in a gateway binary):**

```go
func approveHandler(w http.ResponseWriter, r *http.Request) {
    promiseID := r.URL.Query().Get("id")

    // SDK-codec-compatible encoding: JSON → base64 → quoted string.
    rawJSON, _ := json.Marshal("approved")
    b64 := base64.StdEncoding.EncodeToString(rawJSON)
    quotedB64, _ := json.Marshal(b64)
    val := resonate.Value{Data: json.RawMessage(quotedB64)}

    _, err := resonateClient.Sender().PromiseSettle(r.Context(), resonate.PromiseSettleReq{
        ID:    promiseID,
        State: resonate.SettleStateResolved,
        Value: val,
    })
    // ...
}
```

> Don't reach for `resonate.NewValue` here — there's a codec mismatch with `Future.Await`. See [#22](https://github.com/resonatehq/resonate-sdk-go/issues/22).

**CLI command:**

```sh
resonate promise resolve <promiseID> --data '"approved"'
```

The workflow body itself is identical in every deployment — only the external trigger changes.

## Prerequisites

- Go 1.22+
- The `resonate` server CLI (required only for `-url` mode). Install with Homebrew on macOS or Linux:
  ```
  brew install resonatehq/tap/resonate
  ```
  Other install paths: <https://docs.resonatehq.io/get-started/install>.

## Setup

```sh
git clone https://github.com/resonatehq-examples/example-human-in-the-loop-go.git
cd example-human-in-the-loop-go
go mod download
```

## Run it

### Localnet mode (no server required)

```sh
go run .
```

The simulator goroutine acts as the human reviewer and resolves the promise after ~3 seconds. Total runtime is under 10 seconds.

### Real-server mode

In one terminal, start the dev server:

```sh
resonate dev
```

In another, run the example:

```sh
go run . -url=http://localhost:8001
```

In real-server mode you can kill the worker while the workflow is suspended and restart it — the workflow resumes from the same pending promise rather than starting over.

## What to look for

Expected output (localnet mode):

```
[main] using localnet (in-process, no external server required)
[main] invoking workflow id=hitl-review-<nanos> item=budget-increase-q3
  [workflow] starting review for "budget-increase-q3" (requested by finance-team)
  [workflow] suspended — waiting for external approval
  [workflow] promise ID: hitl-review-<nanos>.1
  [workflow] (in production: expose this ID as an HTTP endpoint or CLI command)
[simulator] received promise ID: hitl-review-<nanos>.1
[simulator] simulating human review delay (~3 seconds)...
[simulator] promise settled — state=resolved
  [workflow] starting review for "budget-increase-q3" (requested by finance-team)
  [workflow] suspended — waiting for external approval
  [workflow] promise ID: hitl-review-<nanos>.1
  [workflow] (in production: expose this ID as an HTTP endpoint or CLI command)
  [workflow] approval received: "approved"
[main] OK: workflow for "budget-increase-q3" completed with decision: approved
```

The workflow body prints twice because durable execution replays the function from the top on resume — all calls before the `Await` re-execute, with `ctx.Promise()` short-circuiting to the already-settled record on the second pass.

The promise ID (`hitl-review-<nanos>.1`) is what a real deployment would surface to the approver. You can inspect it on the dashboard at <http://localhost:8001> when running against a real server.

## File structure

```
example-human-in-the-loop-go/
├── main.go        program entry point
├── go.mod         module declaration + SDK pin
├── go.sum         checksums
├── assets/        README banner images
├── LICENSE        Apache-2.0
└── README.md
```

## Next steps

- [Get started](https://docs.resonatehq.io/get-started) — install paths + first-program walkthrough.
- [Durable execution concepts](https://docs.resonatehq.io/concepts) — what makes invocations durable + how the runtime resumes them.
- [Human-in-the-Loop Pattern](https://docs.resonatehq.io/get-started/examples/human-in-the-loop) — full pattern documentation.
- [`example-durable-sleep-go`](https://github.com/resonatehq-examples/example-durable-sleep-go) — similar suspension mechanics using a timer promise rather than a latent promise.
- **Coming from Temporal?** See [MIGRATING-FROM-TEMPORAL.md](MIGRATING-FROM-TEMPORAL.md) — a side-by-side port of the matching `temporalio/samples-go` example.

## Community

- Discord: <https://resonatehq.io/discord>
- X: <https://x.com/resonatehqio>
- LinkedIn: <https://linkedin.com/company/resonatehq>
- YouTube: <https://youtube.com/@resonatehq>
- Journal: <https://journal.resonatehq.io>

## License

[Apache-2.0](./LICENSE)
