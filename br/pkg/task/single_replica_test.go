// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"bytes"
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

type fakeReplicaConfig struct {
	replicas      int
	rulesEnabled  bool
	ruleCount     int
	failSet       bool
	failRuleTo1   bool
	replicaWrites []int
}

func (f *fakeReplicaConfig) GetReplicateConfig(context.Context) (map[string]any, error) {
	enabled := "false"
	if f.rulesEnabled {
		enabled = "true"
	}
	return map[string]any{"max-replicas": float64(f.replicas), "enable-placement-rules": enabled}, nil
}

func (f *fakeReplicaConfig) SetConfig(_ context.Context, cfg map[string]any, _ ...float64) error {
	n := cfg["max-replicas"].(int)
	if f.failSet && n != 1 {
		return errors.New("pd is down")
	}
	f.replicas = n
	f.replicaWrites = append(f.replicaWrites, n)
	return nil
}

func (f *fakeReplicaConfig) GetPlacementRule(_ context.Context, group, id string) (*pdhttp.Rule, error) {
	return &pdhttp.Rule{GroupID: group, ID: id, Role: pdhttp.Voter, Count: f.ruleCount}, nil
}

func (f *fakeReplicaConfig) SetPlacementRule(_ context.Context, r *pdhttp.Rule) error {
	if f.failRuleTo1 && r.Count == 1 {
		return errors.New("rule rejected")
	}
	f.ruleCount = r.Count
	return nil
}

func startSingleReplica(t *testing.T, f *fakeReplicaConfig) (*singleReplicaRestore, *bytes.Buffer) {
	out := &bytes.Buffer{}
	s := &singleReplicaRestore{pd: f, console: out}
	require.NoError(t, s.enter(context.Background()))
	require.Equal(t, 1, f.replicas)
	return s, out
}

func TestSingleReplicaRestorePutsTheReplicaCountBack(t *testing.T) {
	f := &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3}
	s, out := startSingleReplica(t, f)
	require.Equal(t, 1, f.ruleCount)

	require.NoError(t, s.leave(context.Background(), nil))
	require.Equal(t, []int{1, 3}, f.replicaWrites)
	require.Equal(t, 3, f.ruleCount)
	require.Contains(t, out.String(), "Restored with one replica of each region. The replica count is back to 3.")
	require.Contains(t, out.String(), "no redundancy")
}

func TestSingleReplicaRestoreWithoutPlacementRules(t *testing.T) {
	f := &fakeReplicaConfig{replicas: 5, ruleCount: 7}
	s, _ := startSingleReplica(t, f)
	require.NoError(t, s.leave(context.Background(), nil))
	require.Equal(t, 5, f.replicas)
	require.Equal(t, 7, f.ruleCount)
}

func TestSingleReplicaRestoreFailedRestorePutsTheReplicaCountBack(t *testing.T) {
	f := &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3}
	s, out := startSingleReplica(t, f)
	boom := errors.New("restore failed")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, s.leave(ctx, boom), boom)
	require.Equal(t, 3, f.replicas)
	require.Equal(t, 3, f.ruleCount)
	require.Contains(t, out.String(), "The restore failed. The replica count is back to 3.")
}

func TestSingleReplicaRestoreTellsHowToFixAFailedReset(t *testing.T) {
	f := &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3, failSet: true}
	s, out := startSingleReplica(t, f)

	err := s.leave(context.Background(), nil)
	require.ErrorContains(t, err, "pd is down")
	require.Equal(t, 1, f.replicas)
	require.Contains(t, out.String(), "Set max-replicas to 3 in PD and the count of the default placement rule to 3 by hand.")
}

func TestSingleReplicaRestorePutsACustomRuleBackAsItWas(t *testing.T) {
	f := &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 5}
	s, out := startSingleReplica(t, f)
	require.Equal(t, 1, f.ruleCount)
	require.NoError(t, s.leave(context.Background(), nil))
	require.Equal(t, 3, f.replicas)
	require.Equal(t, 5, f.ruleCount)
	require.Contains(t, out.String(), "The replica count is back to 5.")
}

func TestSingleReplicaRestoreLeavesASingleReplicaClusterAlone(t *testing.T) {
	f := &fakeReplicaConfig{replicas: 1, rulesEnabled: true, ruleCount: 1}
	out := &bytes.Buffer{}
	s := &singleReplicaRestore{pd: f, console: out}
	require.NoError(t, s.enter(context.Background()))
	require.NoError(t, s.leave(context.Background(), nil))
	require.Empty(t, f.replicaWrites)
	require.Contains(t, out.String(), "--single-replica changed nothing")
	require.NotContains(t, out.String(), "no redundancy")
}

func TestSingleReplicaRestoreUndoesAHalfDoneEnter(t *testing.T) {
	f := &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3, failRuleTo1: true}
	s := &singleReplicaRestore{pd: f, console: &bytes.Buffer{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := s.enter(ctx)
	require.ErrorContains(t, err, "rule rejected")
	require.Equal(t, []int{1, 3}, f.replicaWrites)
	require.Equal(t, 3, f.replicas)
	require.Equal(t, 3, f.ruleCount)
}

func TestSingleReplicaRestoreHalfDoneEnterTellsHowToFixAFailedUndo(t *testing.T) {
	f := &fakeReplicaConfig{replicas: 3, rulesEnabled: true, ruleCount: 3, failRuleTo1: true, failSet: true}
	out := &bytes.Buffer{}
	s := &singleReplicaRestore{pd: f, console: out}

	err := s.enter(context.Background())
	require.ErrorContains(t, err, "rule rejected")
	require.ErrorContains(t, err, "pd is down")
	require.Equal(t, 1, f.replicas)
	require.Contains(t, out.String(), "Set max-replicas to 3 in PD and the count of the default placement rule to 3 by hand.")
}
