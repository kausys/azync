package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// defArgs is a plain user task type for the builder tests.
type defArgs struct {
	V string `json:"v"`
}

func (defArgs) Kind() string { return "wf.def.ok" }

// dollarArgs carries a reserved "$" kind, which the builder must reject.
type dollarArgs struct{}

func (dollarArgs) Kind() string { return "$evil" }

func TestValidateAcceptsAWellFormedDAG(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	def := Define("ok").
		Task("a", defArgs{V: "a"}).
		Sleep("wait", time.Minute, After("a")).
		WaitSignal("approved", After("wait")).
		Task("b", defArgs{V: "b"}, After("approved"), Compensate(defArgs{V: "undo"}))
	is.NoError(def.validate())
}

func TestValidateRejectsMalformedDefinitions(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		def  *Definition
		want string
	}{
		"no name": {
			def:  Define("").Task("a", defArgs{}),
			want: "has no name",
		},
		"no tasks": {
			def:  Define("empty"),
			want: `"empty" declares no tasks`,
		},
		"empty key": {
			def:  Define("wf").Task("", defArgs{}),
			want: "empty key",
		},
		"duplicate key": {
			def:  Define("wf").Task("a", defArgs{}).Task("a", defArgs{}),
			want: `duplicate task key "a"`,
		},
		"reserved dollar key": {
			def:  Define("wf").Task("$x", defArgs{}),
			want: `task key "$x" uses the reserved "$" prefix`,
		},
		"reserved comp key": {
			def:  Define("wf").Task("comp:x", defArgs{}),
			want: `task key "comp:x" uses the reserved "comp:" prefix`,
		},
		"nil args": {
			def:  Define("wf").Task("a", nil),
			want: `task "a" has nil args`,
		},
		"reserved dollar kind": {
			def:  Define("wf").Task("a", dollarArgs{}),
			want: `kind "$evil" uses the reserved "$" prefix`,
		},
		"reserved compensation kind": {
			def:  Define("wf").Task("a", defArgs{}, Compensate(dollarArgs{})),
			want: `compensation kind "$evil" uses the reserved "$" prefix`,
		},
		"non-positive sleep": {
			def:  Define("wf").Sleep("s", 0),
			want: `sleep "s" duration must be positive`,
		},
		"dependency on unknown key": {
			def:  Define("wf").Task("a", defArgs{}, After("ghost")),
			want: `task "a" depends on unknown key "ghost"`,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			is := require.New(t)
			err := tc.def.validate()
			is.Error(err)
			is.Contains(err.Error(), tc.want)
			is.Contains(err.Error(), "workflow:")
		})
	}
}

func TestValidateRejectsCycles(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	// a -> b -> c -> a is a cycle; none has indegree 0 so toposort strands them.
	def := Define("cyclic").
		Task("a", defArgs{}, After("c")).
		Task("b", defArgs{}, After("a")).
		Task("c", defArgs{}, After("b"))
	err := def.validate()
	is.Error(err)
	is.Contains(err.Error(), "dependency cycle")
	is.Contains(err.Error(), "a")
	is.Contains(err.Error(), "b")
	is.Contains(err.Error(), "c")
}

func TestValidateRejectsSelfDependency(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	def := Define("self").Task("a", defArgs{}, After("a"))
	err := def.validate()
	is.Error(err)
	is.Contains(err.Error(), "dependency cycle")
}

// TestRunValidatesBeforeTouchingTheStore proves validation is the gate in
// Client.Run: an invalid definition fails without inserting anything.
func TestRunValidatesBeforeTouchingTheStore(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	_, err := r.Client().Run(context.Background(),
		Define("bad").Task("a", defArgs{}, After("missing")))
	is.Error(err)
	is.Contains(err.Error(), "unknown key")

	page, err := r.Manager().List(context.Background(), Filter{}, 0, 50)
	is.NoError(err)
	is.Zero(page.Total, "a definition that fails validation must insert nothing")
}

func TestRunRejectsNilDefinition(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	_, err := r.Client().Run(context.Background(), nil)
	is.Error(err)
	is.Contains(err.Error(), "definition is nil")
}
