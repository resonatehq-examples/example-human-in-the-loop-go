// Package main demonstrates the human-in-the-loop pattern with the Resonate
// Go SDK.
//
// A workflow calls [resonate.Context.Promise] to create a latent durable
// promise — one with no registered function behind it. The promise only
// resolves when an external actor calls the Resonate promise-settle API with
// the promise ID. [Future.Await] parks the workflow goroutine until that
// settlement arrives, surviving any number of worker crashes or restarts in
// between.
//
// # How the simulation works
//
// In this single-process demo, "the human" is played by a goroutine that wakes
// up after ~3 seconds and calls [resonate.Sender.PromiseSettle] directly
// through the SDK's internal network. No HTTP server is needed.
//
// The workflow writes its promise ID to a shared channel right before it parks.
// The simulator goroutine reads that ID, waits, then resolves the promise.
// On replay (after the settle unblocks the runtime), the same promise ID is
// seen again; the [sync.Once] gate prevents a double-settle.
//
// # Production pattern
//
// In a real deployment the simulator goroutine would be replaced by something
// like:
//
//   - An HTTP endpoint: POST /approve?id=<promiseID>
//   - A CLI command: resonate promise resolve <promiseID>
//   - A scheduled job, a notification callback, or a Slack slash command
//
// The workflow body itself is identical in every case — the only difference is
// what external actor calls promise.settle.
//
// # Modes
//
// By default the program uses localnet — an in-process transport that needs no
// external server. Localnet is convenient for local development and testing.
// To use a real Resonate server instead:
//
//	resonate dev                       # terminal 1
//	go run . -url=http://localhost:8001 # terminal 2
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
	"github.com/resonatehq/resonate-sdk-go/localnet"
)

// ReviewRequest is the argument type for the approval workflow. It carries the
// item that needs human review and the requester's identity.
type ReviewRequest struct {
	Item      string `json:"item"`
	Requester string `json:"requester"`
}

// approvalWorkflow suspends until an external actor resolves the latent
// durable promise it creates, then returns the decision that was encoded in
// the settlement value.
//
// promiseIDs is a buffered channel (capacity 1) used to hand the latent
// promise's ID to the simulator goroutine. Using a closure-captured channel
// avoids any global state while keeping the workflow signature compatible with
// [resonate.Register].
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

// promiseIDs is the channel through which the workflow hands its latent
// promise ID to the simulator goroutine. Capacity 1 so the workflow body can
// send without blocking even when the simulator has not yet read.
var promiseIDs = make(chan string, 1)

func main() {
	serverURL := flag.String("url", "", "Resonate server URL (e.g. http://localhost:8001). Omit to use localnet.")
	flag.Parse()

	var cfg resonate.Config

	if *serverURL != "" {
		cfg = resonate.Config{URL: *serverURL}
		fmt.Printf("[main] connecting to server at %s\n", *serverURL)
	} else {
		// Localnet mode: in-process transport, no external server needed.
		// NoopHeartbeat is required — localnet has no HTTP endpoint for the
		// default AsyncHeartbeat to reach.
		pid := "hitl-worker"
		cfg = resonate.Config{
			Network:   localnet.NewLocal("default", &pid),
			Heartbeat: resonate.NoopHeartbeat{},
			TTL:       5 * time.Minute,
		}
		fmt.Println("[main] using localnet (in-process, no external server required)")
	}

	r, err := resonate.New(cfg)
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	approvalFn, err := resonate.Register(r, "approvalWorkflow", approvalWorkflow)
	if err != nil {
		log.Fatalf("Register: %v", err)
	}

	// Start the simulator goroutine BEFORE running the workflow so it is ready
	// to receive the promise ID as soon as the workflow suspends.
	var simulatorOnce sync.Once
	go func() {
		// Block until the workflow hands us its promise ID.
		promiseID := <-promiseIDs

		// simulatorOnce guards against a second invocation if the goroutine
		// is somehow triggered twice (should not happen in practice).
		simulatorOnce.Do(func() {
			fmt.Printf("[simulator] received promise ID: %s\n", promiseID)
			fmt.Println("[simulator] simulating human review delay (~3 seconds)...")
			time.Sleep(3 * time.Second)

			// Build the settlement value. In production this carries whatever
			// the human entered — "approved", "rejected", a comment, etc.
			decision := "approved"

			// The SDK codec encodes values as: JSON → base64 → quoted string
			// stored in Value.Data. We replicate that here because Resonate
			// does not expose a public Codec() accessor on the Resonate type
			// (resonate-sdk-go issue #9 — the missing Promises sub-client
			// would provide a higher-level Resolve(id, v) that handles this
			// encoding automatically).
			//
			// Friction note: callers who try resonate.NewValue(decision)
			// instead get a Value whose Data is the raw JSON string —
			// NOT base64-wrapped — which causes a decode error on the
			// workflow side when Codec.Decode tries to base64-unmarshal it.
			// There is no compile-time or runtime warning; the failure only
			// surfaces as a DecodingError inside Future.Await.
			rawJSON, marshalErr := json.Marshal(decision)
			if marshalErr != nil {
				log.Printf("[simulator] ERROR marshaling decision: %v", marshalErr)
				return
			}
			b64 := base64.StdEncoding.EncodeToString(rawJSON)
			quotedB64, marshalErr := json.Marshal(b64)
			if marshalErr != nil {
				log.Printf("[simulator] ERROR quoting base64: %v", marshalErr)
				return
			}
			val := resonate.Value{Data: json.RawMessage(quotedB64)}

			// PromiseSettle is the external-resolution API. In production this
			// call lives behind an HTTP handler, a CLI subcommand, or any other
			// mechanism that accepts the promise ID and a resolution payload.
			//
			// Friction note (resonate-sdk-go issue #9): PromiseSettle lives on
			// the internal Sender type, reached via r.Sender(). There is no
			// higher-level Resonate.Promises sub-client yet. A future
			// Promises().Resolve(id, value) API would handle both the encoding
			// and the RPC in a single call.
			settleReq := resonate.PromiseSettleReq{
				ID:    promiseID,
				State: resonate.SettleStateResolved,
				Value: val,
			}

			ctx := context.Background()
			rec, settleErr := r.Sender().PromiseSettle(ctx, settleReq)
			if settleErr != nil {
				log.Printf("[simulator] ERROR settling promise: %v", settleErr)
				return
			}
			fmt.Printf("[simulator] promise settled — state=%s\n", rec.State)
		})
	}()

	ctx := context.Background()
	id := fmt.Sprintf("hitl-review-%d", time.Now().UnixNano())
	req := ReviewRequest{
		Item:      "budget-increase-q3",
		Requester: "finance-team",
	}

	fmt.Printf("[main] invoking workflow id=%s item=%s\n", id, req.Item)

	h, err := approvalFn.Run(ctx, id, req)
	if err != nil {
		log.Fatalf("Run: %v", err)
	}

	result, err := h.Result(ctx)
	if err != nil {
		log.Fatalf("Result: %v", err)
	}

	fmt.Printf("[main] OK: %s\n", result)
}
