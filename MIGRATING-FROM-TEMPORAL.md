# Coming from Temporal: Human-in-the-Loop (Await Signals)

This guide is for developers who know the [`temporalio/samples-go/await-signals`](https://github.com/temporalio/samples-go/tree/main/await-signals) pattern and want to understand how it maps to this Resonate example. The goal is a concrete, side-by-side port — not a rehash of the concept documentation.

## The pattern

Both systems solve the same problem: a workflow needs to pause mid-execution and wait for something outside the process — a human decision, an external event, a third-party callback — before it can continue. Temporal models this as a named signal channel with a listener goroutine and condition variables. Resonate models it as a single latent durable promise: a promise with no registered function behind it that only settles when an external caller resolves it by ID.

## Side by side

### Temporal (`samples-go/await-signals`)

```go
// await-signals/await_signals_workflow.go

type AwaitSignals struct {
	FirstSignalTime time.Time
	Signal1Received bool
	Signal2Received bool
	Signal3Received bool
}

// Listen to signals Signal1, Signal2, and Signal3
func (a *AwaitSignals) Listen(ctx workflow.Context) {
	for {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(workflow.GetSignalChannel(ctx, "Signal1"), func(c workflow.ReceiveChannel, more bool) {
			c.Receive(ctx, nil)
			a.Signal1Received = true
			// (logging omitted for brevity)
		})
		// ... Signal2/Signal3 follow the same shape
		selector.Select(ctx)
		if a.FirstSignalTime.IsZero() {
			a.FirstSignalTime = workflow.Now(ctx)
		}
	}
}

// AwaitSignalsWorkflow workflow definition
func AwaitSignalsWorkflow(ctx workflow.Context) (err error) {
	var a AwaitSignals
	// Listen to signals in a different goroutine
	workflow.Go(ctx, a.Listen)

	// Wait for Signal1
	err = workflow.Await(ctx, func() bool {
		return a.Signal1Received
	})
	if err != nil {
		return
	}

	// Wait for Signal2 (with timeout)
	timeout, err := a.GetNextTimeout(ctx)
	if err != nil {
		return
	}
	ok, err := workflow.AwaitWithTimeout(ctx, timeout, func() bool {
		return a.Signal2Received
	})
	if !ok {
		return temporal.NewApplicationError("Timed out waiting for signal2", "timeout")
	}

	// ... Signal3 follows the same shape as Signal2 (full block omitted for brevity:
	// GetNextTimeout + AwaitWithTimeout + error checks)
	return nil
}
```

The external caller delivers a signal via the Temporal client:

```go
// In the starter / external system:
err = temporalClient.SignalWorkflow(ctx, workflowID, runID, "Signal1", nil)
```

### Resonate (this example)

```go
func approvalWorkflow(ctx *resonate.Context, req ReviewRequest) (string, error) {
	fmt.Printf("  [workflow] starting review for %q (requested by %s)\n",
		req.Item, req.Requester)

	// Create a latent durable promise. No function is registered behind it;
	// the promise will only settle when promise.settle is called externally.
	// Future.ID() is the identifier the external actor needs.
	f, err := ctx.Promise()
	if err != nil {
		return "", fmt.Errorf("ctx.Promise: %w", err)
	}

	promiseID := f.ID()
	fmt.Printf("  [workflow] suspended — waiting for external approval\n")
	fmt.Printf("  [workflow] promise ID: %s\n", promiseID)
	fmt.Printf("  [workflow] (in production: expose this ID as an HTTP endpoint or CLI command)\n")

	// Hand the ID to the simulator. The channel has capacity 1; on workflow
	// replay the same ID is produced again, so we use a non-blocking send to
	// avoid deadlocking if the simulator has already consumed the first send.
	select {
	case promiseIDs <- promiseID:
	default:
		// Simulator already has the ID from a previous pass. No-op.
	}

	// Await parks the workflow goroutine on the latent promise. When the
	// promise is still pending this triggers the durable-suspension mechanism
	// internally and the workflow body will be re-entered once the promise
	// settles. The string value encoded by the settler is decoded into
	// `decision` here.
	var decision string
	if err := f.Await(&decision); err != nil {
		return "", fmt.Errorf("await approval: %w", err)
	}

	fmt.Printf("  [workflow] approval received: %q\n", decision)
	return fmt.Sprintf("workflow for %q completed with decision: %s", req.Item, decision), nil
}
```

The external caller (here, a simulator goroutine) settles the promise via the SDK:

```go
// Encode the value the same way the SDK's codec encodes values:
// JSON → base64 → quoted string, stored in Value.Data.
rawJSON, _ := json.Marshal("approved")
b64 := base64.StdEncoding.EncodeToString(rawJSON)
quotedB64, _ := json.Marshal(b64)
val := resonate.Value{Data: json.RawMessage(quotedB64)}

settleReq := resonate.PromiseSettleReq{
	ID:    promiseID,
	State: resonate.SettleStateResolved,
	Value: val,
}
rec, err := r.Sender().PromiseSettle(ctx, settleReq)
```

Or via the CLI (no Go code needed for the external actor):

```sh
resonate promise resolve <promiseID> --data '"approved"'
```

## Concept mapping

| Temporal | Resonate | Notes |
|---|---|---|
| `workflow.GetSignalChannel(ctx, "Signal1")` | `ctx.Promise()` | One latent promise replaces the signal name definition |
| `selector.AddReceive(ch, handler)` | — | No listener goroutine or selector needed |
| `a.Signal1Received = true` (flag in handler) | — | The promise record itself carries the resolved value; no flag variable |
| `workflow.Go(ctx, a.Listen)` | — | No background goroutine; the SDK handles suspension internally |
| `workflow.Await(ctx, func() bool { return a.Signal1Received })` | `f.Await(&decision)` | Both park the workflow; Resonate decodes the settled value directly into `decision` |
| `workflow.AwaitWithTimeout(ctx, timeout, cond)` | `f.Await(&decision)` + `ctx.Promise(resonate.PromiseOpts{Timeout: d})` | Timeout via promise expiry rather than a workflow-side condition; per-promise deadline set at creation |
| `client.SignalWorkflow(ctx, wfID, runID, "Signal1", payload)` | `r.Sender().PromiseSettle(ctx, req)` | See Notes & coverage for the encoding requirement |
| Workflow + Worker registration + task queue wiring | `resonate.Register(r, "approvalWorkflow", approvalWorkflow)` | No task queue; the function name is the dispatch key |

## Porting it, step by step

1. **Remove the listener goroutine.** Delete the `AwaitSignals` struct, the `Listen` method, the `workflow.Go(ctx, a.Listen)` call, and all `Signal*Received` flag variables. Resonate does not use signal channels.

2. **Replace each `workflow.Await(cond)` with `ctx.Promise()` + `f.Await(&value)`.** Where you had `workflow.GetSignalChannel` + a handler that sets a flag + `workflow.Await` on that flag, you now have a single call to `ctx.Promise()` that returns a `Future`. The settlement value is decoded directly into the variable you pass to `f.Await`.

3. **Expose the promise ID.** Call `f.ID()` immediately after `ctx.Promise()`. This ID is what the external actor needs — surface it however makes sense: log it, write it to a database, embed it in a notification email, expose it via an HTTP endpoint query parameter.

4. **Replace `client.SignalWorkflow` with a promise settle call.** The external actor (HTTP handler, CLI, Slack bot, etc.) encodes the value and calls `PromiseSettle` rather than sending a Temporal signal. See the encoding pattern in the code above and the Notes section below.

5. **Construct the Resonate instance.** Before registering anything, create `*Resonate`:

   ```go
   // Real server:
   r, err := resonate.New(resonate.Config{URL: serverURL})
   // Localnet (in-process, no external server):
   // r, err := resonate.New(resonate.Config{Network: localnet.NewLocal("default", &pid), Heartbeat: resonate.NoopHeartbeat{}, TTL: 5*time.Minute})
   if err != nil {
       log.Fatalf("resonate.New: %v", err)
   }
   defer func() { _ = r.Stop() }()
   ```

6. **Drop the Temporal Worker / task-queue setup.** In Temporal you configure a Worker with a task queue and register workflow + activity types. In Resonate you call `resonate.Register(r, name, fn)` — no task queue, no type registry, no activity-options struct.

7. **Wire the runner.** Replace `temporalClient.ExecuteWorkflow(...)` with `approvalFn.Run(ctx, id, req)` and `result, err := h.Result(ctx)` to collect the final value. Note the type distinction: `approvalFn.Run` returns `*TypedHandle[R]`, whose `Result(ctx)` returns `(R, error)` directly. If you use `r.Get(ctx, id)` instead, that returns an untyped `*Handle` whose `Result(ctx, &out)` signature takes an output pointer and returns only `error`.

## What's different (and why)

**One concept instead of four.** In `await-signals`, handling an external event requires: (1) a named signal channel, (2) a listener goroutine, (3) a flag variable, and (4) a `workflow.Await` condition. The sample's own comments present this listener-goroutine/flag pattern as the readable, idiomatic approach in Temporal — it is not accidental complexity. Resonate collapses all four into a single `ctx.Promise()` call because the durable promise record in the server IS the pending state; there is no in-process flag to maintain.

**Value-carrying vs. notification-only.** In this sample the `await-signals` signals carry no data — `c.Receive(ctx, nil)` passes `nil` because the signal is used purely as a trigger. Temporal's `ReceiveChannel.Receive` can accept a typed pointer and decode arbitrary payloads; the nil here is a sample choice, not a platform constraint. Resonate's settlement value travels with the resolve call and is decoded directly into the `f.Await` target variable. This is a small convenience for approval-style workflows where the decision itself (e.g. `"approved"` / `"rejected"`) is the payload.

**Replay semantics.** Both systems replay the workflow function from the top on resume. In Temporal, the listener goroutine re-runs and the Selector re-registers, but signals already received are replayed from history without blocking. In Resonate, `ctx.Promise()` on replay sees the promise is already settled and short-circuits immediately — the `f.Await` call returns without parking. The `promiseIDs` channel in this example uses a non-blocking send specifically to handle this: on the replay pass, the simulator goroutine has already consumed the ID, so the send is a no-op.

**No `@workflow` / `@activity` split.** The `await-signals` sample has no activities or child workflows, so there are no `ActivityOptions` or `ChildWorkflowOptions` in play here. What it does require is task-queue wiring and a `w.RegisterWorkflow` call. Resonate replaces those two pieces with a single `resonate.Register(r, "approvalWorkflow", approvalWorkflow)` call — no task queue, no type registry.

**Timeout model.** `await-signals` enforces two timeouts (signal-to-signal and total elapsed) using `workflow.AwaitWithTimeout` with computed durations. Resonate does not have a built-in `AwaitWithTimeout`; instead, you set a deadline on the latent promise at creation time. You can cap a latent promise's deadline per-call with `ctx.Promise(resonate.PromiseOpts{Timeout: d})`. If you leave it unset, the SDK computes `min(now + DefaultChildTimeout (24h), parent.timeoutAt)` — in other words the child starts with up to 24 hours but is capped at the parent's absolute deadline, whichever is sooner. If the promise expires before settlement, `f.Await` returns an error.

## Notes & coverage

**The resolve path is lower-level than you might expect.** There is no high-level `r.Promises().Resolve(id, value)` method in the Go SDK yet ([resonate-sdk-go#28](https://github.com/resonatehq/resonate-sdk-go/issues/28)). To settle a promise programmatically you must reach `r.Sender().PromiseSettle(...)` and encode the value manually. The SDK's codec represents values as: JSON-encode the Go value → base64-encode the JSON bytes → JSON-encode the base64 string (to produce a quoted string) → store as `Value.Data`. In full:

```go
rawJSON, _ := json.Marshal(decision)                         // e.g. `"approved"`
b64 := base64.StdEncoding.EncodeToString(rawJSON)            // base64 of that JSON
quotedB64, _ := json.Marshal(b64)                            // quoted base64 string
val := resonate.Value{Data: json.RawMessage(quotedB64)}
```

**Do not use `resonate.NewValue`** for this. `NewValue` stores the raw JSON in `Value.Data` without the base64 wrapper, which causes a decode error inside `Future.Await`. There is no compile-time warning; the failure surfaces as a `DecodingError` at runtime.

**CLI path skips the encoding.** If the external actor is a human or a script rather than Go code, the `resonate` CLI handles encoding automatically:

```sh
resonate promise resolve <promiseID> --data '"approved"'
```

Pass a JSON-encoded value to `--data`. The CLI wraps it correctly before sending to the server.

**The `await-signals` sample handles multiple sequential signals.** This example demonstrates a single approval gate (one latent promise, one external settlement). If you need multiple sequential external events — the `await-signals` pattern with Signal1 → Signal2 → Signal3 — create one `ctx.Promise()` per gate, sequentially in the workflow body. Each promise gets a distinct ID. You can surface all IDs upfront or reveal them one step at a time.

**In-process vs. external settlement.** This example's simulator goroutine calls `PromiseSettle` from within the same process as the worker, using `r.Sender()` directly. In production the settlement call typically lives in a separate HTTP gateway binary that holds its own `resonate.New(cfg)` instance (with the same server URL). The workflow binary and the gateway binary never need to share code — they only share the promise ID.

## Further reading

- Concept-level guide (all SDKs): https://docs.resonatehq.io/evaluate/coming-from/temporal
- Temporal sample: https://github.com/temporalio/samples-go/tree/main/await-signals
- This example's README
