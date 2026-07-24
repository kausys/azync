package workflow

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kausys/azync/driver"
)

// TaskArgs identifies a task's unit of work by its wire-stable Kind (decoupled
// from the Go type path), e.g. "kyc.submit_verification" — the same contract
// as the queue's JobArgs.
type TaskArgs interface {
	Kind() string
}

// FailurePolicy is a workflow's declared reaction to a dead task (a task that
// aborted or exhausted its retry budget).
type FailurePolicy = driver.OnFailurePolicy

const (
	// Cancel is the default failure policy: cancel the remaining tasks, run the
	// compensations of the succeeded tasks that declared one (in reverse
	// completion order), and settle the workflow failed.
	Cancel FailurePolicy = driver.OnFailureCancel
	// Suspend parks the workflow as suspended, leaving its tasks untouched, so
	// an operator decides through the Manager between Retry, Compensate and
	// Cancel.
	Suspend FailurePolicy = driver.OnFailureSuspend
)

// Definition is a declared workflow: a name, a failure policy and a static DAG
// of tasks built with Task, Sleep and WaitSignal. Build one with Define; run
// it with Client.Run, which validates the DAG and inserts it atomically. A
// Definition is a template: it is not mutated by Run and may be reused across
// runs.
type Definition struct {
	name      string
	onFailure FailurePolicy
	meta      map[string]string
	tasks     []taskDecl
}

// taskDecl is one declared task of the DAG, before validation.
type taskDecl struct {
	key            string
	kind           string
	args           TaskArgs // nil for Sleep and WaitSignal
	sleepFor       time.Duration
	after          []string
	compensate     TaskArgs
	maxRetries     int
	ignoreDeadDeps bool
}

// DefineOption customizes a Definition.
type DefineOption func(*Definition)

// OnFailure declares the workflow's failure policy (default Cancel).
func OnFailure(policy FailurePolicy) DefineOption {
	return func(d *Definition) { d.onFailure = policy }
}

// WithMeta attaches one string-valued annotation to the workflow (repeatable).
// Meta is stamped onto the workflow header and onto every task job, and is
// readable from handlers through Metadata. Run-time entries added with
// WithRunMeta override definition entries on key conflicts.
func WithMeta(key, value string) DefineOption {
	return func(d *Definition) {
		if d.meta == nil {
			d.meta = map[string]string{}
		}
		d.meta[key] = value
	}
}

// TaskOption customizes one declared task.
type TaskOption func(*taskDecl)

// After declares dependencies: the task stays blocked until every named task
// succeeded (repeatable; keys accumulate). A dependency on a missing key, and
// any dependency cycle, is rejected by Client.Run.
func After(keys ...string) TaskOption {
	return func(t *taskDecl) { t.after = append(t.after, keys...) }
}

// Compensate declares the task's compensation: when the workflow compensates
// (Cancel policy, Manager.Compensate) and this task had succeeded, a
// "comp:<key>" task of args.Kind() runs with args as its payload,
// chained in reverse completion order with the other compensations.
func Compensate(args TaskArgs) TaskOption {
	return func(t *taskDecl) { t.compensate = args }
}

// MaxRetries overrides the retry budget for this task (highest precedence:
// task option > register option > runtime default).
func MaxRetries(n int) TaskOption {
	return func(t *taskDecl) {
		if n > 0 {
			t.maxRetries = n
		}
	}
}

// IgnoreDeadDeps lets the task run even when a dependency ended dead or
// cancelled, treating it as satisfied. When every dependent of a dead task
// declares it, the failure policy does not fire for that death and the
// tolerant branch keeps running — but a workflow that completes with any dead
// task still settles failed, never succeeded. The exemption is never vacuous:
// a dead task with no dependents always triggers the policy.
func IgnoreDeadDeps() TaskOption {
	return func(t *taskDecl) { t.ignoreDeadDeps = true }
}

// Define starts a workflow definition. Add tasks with Task, Sleep and
// WaitSignal; validation happens in Client.Run, not here.
func Define(name string, opts ...DefineOption) *Definition {
	d := &Definition{name: name, onFailure: Cancel}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Task declares one handler-backed task: key identifies it within the DAG and
// args carries its kind and payload (the handler registered for args.Kind()
// executes it). It returns the Definition for chaining.
func (d *Definition) Task(key string, args TaskArgs, opts ...TaskOption) *Definition {
	return d.add(taskDecl{key: key, args: args}, opts)
}

// Sleep declares a durable timer task: once its dependencies are satisfied it
// waits dur (resolved against the backend clock) and then succeeds, without
// running any handler. A signal named after the task's key wakes it early
// (Client.Signal(id, key, ...)). It returns the Definition for chaining.
func (d *Definition) Sleep(key string, dur time.Duration, opts ...TaskOption) *Definition {
	return d.add(taskDecl{key: key, kind: driver.KindSleep, sleepFor: dur}, opts)
}

// WaitSignal declares a wait-for-signal task: once its dependencies are
// satisfied it parks until Client.Signal(id, key, payload) completes it, with
// the payload persisted as its result (readable downstream through ResultOf).
// The signal name is the task's key. It returns the Definition for chaining.
func (d *Definition) WaitSignal(key string, opts ...TaskOption) *Definition {
	return d.add(taskDecl{key: key, kind: driver.KindSignal}, opts)
}

func (d *Definition) add(t taskDecl, opts []TaskOption) *Definition {
	for _, opt := range opts {
		opt(&t)
	}
	d.tasks = append(d.tasks, t)
	return d
}

// validate checks the whole definition: it is the single gate Client.Run runs
// before building driver params. Every error names the offending task key.
func (d *Definition) validate() error {
	if d.name == "" {
		return errors.New("workflow: definition has no name")
	}
	if len(d.tasks) == 0 {
		return fmt.Errorf("workflow: definition %q declares no tasks", d.name)
	}

	keys := make(map[string]bool, len(d.tasks))
	for _, t := range d.tasks {
		if err := d.validateTask(t, keys); err != nil {
			return err
		}
		keys[t.key] = true
	}
	for _, t := range d.tasks {
		for _, dep := range t.after {
			if !keys[dep] {
				return fmt.Errorf("workflow: definition %q: task %q depends on unknown key %q", d.name, t.key, dep)
			}
		}
	}
	return d.validateAcyclic()
}

// validateTask checks one declaration against the keys seen so far.
func (d *Definition) validateTask(t taskDecl, seen map[string]bool) error {
	switch {
	case t.key == "":
		return fmt.Errorf("workflow: definition %q: a task has an empty key", d.name)
	case seen[t.key]:
		return fmt.Errorf("workflow: definition %q: duplicate task key %q", d.name, t.key)
	case strings.HasPrefix(t.key, "$"):
		return fmt.Errorf("workflow: definition %q: task key %q uses the reserved \"$\" prefix", d.name, t.key)
	case strings.HasPrefix(t.key, driver.TaskKeyCompensationPrefix):
		return fmt.Errorf("workflow: definition %q: task key %q uses the reserved %q prefix",
			d.name, t.key, driver.TaskKeyCompensationPrefix)
	}

	switch t.kind {
	case driver.KindSleep:
		if t.sleepFor <= 0 {
			return fmt.Errorf("workflow: definition %q: sleep %q duration must be positive, got %v",
				d.name, t.key, t.sleepFor)
		}
	case driver.KindSignal:
		// Nothing beyond the shared key rules.
	default:
		if t.args == nil {
			return fmt.Errorf("workflow: definition %q: task %q has nil args", d.name, t.key)
		}
		if strings.HasPrefix(t.args.Kind(), "$") {
			return fmt.Errorf("workflow: definition %q: task %q kind %q uses the reserved \"$\" prefix",
				d.name, t.key, t.args.Kind())
		}
	}

	if t.compensate != nil && strings.HasPrefix(t.compensate.Kind(), "$") {
		return fmt.Errorf("workflow: definition %q: task %q compensation kind %q uses the reserved \"$\" prefix",
			d.name, t.key, t.compensate.Kind())
	}
	return nil
}

// validateAcyclic runs a Kahn toposort over the declared edges and rejects any
// cycle, naming the keys trapped in it.
func (d *Definition) validateAcyclic() error {
	indegree := make(map[string]int, len(d.tasks))
	dependents := make(map[string][]string, len(d.tasks))
	for _, t := range d.tasks {
		indegree[t.key] += 0
		for _, dep := range t.after {
			indegree[t.key]++
			dependents[dep] = append(dependents[dep], t.key)
		}
	}

	queue := make([]string, 0, len(d.tasks))
	for _, t := range d.tasks {
		if indegree[t.key] == 0 {
			queue = append(queue, t.key)
		}
	}
	resolved := 0
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		resolved++
		for _, dep := range dependents[key] {
			indegree[dep]--
			if indegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}
	if resolved == len(d.tasks) {
		return nil
	}

	cyclic := make([]string, 0, len(d.tasks)-resolved)
	for _, t := range d.tasks {
		if indegree[t.key] > 0 {
			cyclic = append(cyclic, t.key)
		}
	}
	return fmt.Errorf("workflow: definition %q: dependency cycle involving %s",
		d.name, strings.Join(cyclic, ", "))
}
