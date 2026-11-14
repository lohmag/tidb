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
	"context"
	"fmt"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/restore"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const (
	flagSingleReplica = "single-replica"

	singleReplicaResetTimeout = time.Minute
)

// replicaConfigClient is the part of the PD client the single replica restore
// changes the cluster with.
type replicaConfigClient interface {
	GetReplicateConfig(context.Context) (map[string]any, error)
	SetConfig(context.Context, map[string]any, ...float64) error
	GetPlacementRule(context.Context, string, string) (*pdhttp.Rule, error)
	SetPlacementRule(context.Context, *pdhttp.Rule) error
}

// singleReplicaRestore lowers the cluster to one replica for the restore and
// puts the replica count back afterwards. PD then adds the other replicas in
// the background at its own pace.
type singleReplicaRestore struct {
	pd      replicaConfigClient
	console io.Writer

	replicas     int
	rulesEnabled bool
	defaultRule  *pdhttp.Rule
}

// enterSingleReplicaRestore checks the target cluster holds no user table, as
// lowering the replicas also drops them from data that is already there, and
// keeps one replica of each region.
func enterSingleReplicaRestore(ctx context.Context, mgr *conn.Mgr, console io.Writer) (*singleReplicaRestore, error) {
	if err := restore.AssertUserDBsEmpty(mgr.GetDomain()); err != nil {
		return nil, errors.Annotate(err, "single replica restore needs a cluster without user tables")
	}
	s := &singleReplicaRestore{pd: mgr.GetPDHTTPClient(), console: console}
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// enter keeps one replica of each region. leave must run after it however the
// restore ends.
func (s *singleReplicaRestore) enter(ctx context.Context) error {
	cfg, err := s.pd.GetReplicateConfig(ctx)
	if err != nil {
		return errors.Annotate(err, "single replica: read the replication config")
	}
	replicas, ok := cfg["max-replicas"].(float64)
	if !ok {
		return errors.Errorf("single replica: unexpected max-replicas %v", cfg["max-replicas"])
	}
	s.replicas = int(replicas)
	s.rulesEnabled = fmt.Sprint(cfg["enable-placement-rules"]) == "true"
	if s.rulesEnabled {
		if s.defaultRule, err = s.pd.GetPlacementRule(ctx, "pd", "default"); err != nil {
			return errors.Annotate(err, "single replica: read the default placement rule")
		}
	}
	if s.alreadySingle() {
		log.Info("single replica restore: the cluster already keeps one replica of each region")
		return nil
	}
	log.Info("single replica restore: keeping one replica of each region during the restore",
		zap.Int("replicas", s.replicas), zap.Bool("placement-rules", s.rulesEnabled))
	if err := s.pd.SetConfig(ctx, map[string]any{"max-replicas": 1}); err != nil {
		// PD may have applied it and only the response was lost.
		return s.rollback(ctx, errors.Annotate(err, "single replica: set max-replicas to 1"))
	}
	if s.rulesEnabled {
		rule := *s.defaultRule
		rule.Count = 1
		if err := s.pd.SetPlacementRule(ctx, &rule); err != nil {
			return s.rollback(ctx, errors.Annotate(err, "single replica: set the default placement rule to 1 peer"))
		}
	}
	return nil
}

// rollback puts back what enter already changed when enter fails half way.
func (s *singleReplicaRestore) rollback(ctx context.Context, enterErr error) error {
	resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleReplicaResetTimeout)
	defer cancel()
	if err := s.putBack(resetCtx); err != nil {
		s.sayResetFailed(err)
		return multierr.Append(enterErr, err)
	}
	return enterErr
}

// effectiveReplicas is the count PD keeps: the default placement rule's when
// placement rules are on, max-replicas otherwise.
func (s *singleReplicaRestore) effectiveReplicas() int {
	if s.rulesEnabled {
		return s.defaultRule.Count
	}
	return s.replicas
}

func (s *singleReplicaRestore) alreadySingle() bool {
	return s.replicas <= 1 && (!s.rulesEnabled || s.defaultRule.Count <= 1)
}

// putBack restores max-replicas and the default placement rule as they were.
func (s *singleReplicaRestore) putBack(ctx context.Context) error {
	if err := s.pd.SetConfig(ctx, map[string]any{"max-replicas": s.replicas}); err != nil {
		return errors.Annotatef(err, "single replica: set max-replicas back to %d", s.replicas)
	}
	if s.rulesEnabled {
		if err := s.pd.SetPlacementRule(ctx, s.defaultRule); err != nil {
			return errors.Annotatef(err, "single replica: put the default placement rule back to %d peers", s.defaultRule.Count)
		}
	}
	return nil
}

// leave puts the replica count back, also after a failed or cancelled restore.
func (s *singleReplicaRestore) leave(ctx context.Context, restoreErr error) error {
	if s.alreadySingle() {
		s.say("The cluster keeps one replica of each region, --single-replica changed nothing.")
		return restoreErr
	}
	resetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleReplicaResetTimeout)
	defer cancel()
	if err := s.putBack(resetCtx); err != nil {
		s.sayResetFailed(err)
		return multierr.Append(restoreErr, err)
	}
	msg := "PD is adding the other replicas in the background. Until it finishes the cluster has no redundancy: " +
		"losing a TiKV loses the regions it holds. `pd-ctl region check miss-peer` lists the regions still waiting."
	if restoreErr != nil {
		log.Warn("single replica restore: the restore failed, the replica count is back",
			zap.Int("replicas", s.replicas))
		s.say("The restore failed. The replica count is back to %d. %s", s.effectiveReplicas(), msg)
		return restoreErr
	}
	log.Warn("single replica restore: restored with one replica, PD is adding the other replicas",
		zap.Int("replicas", s.replicas))
	s.say("Restored with one replica of each region. The replica count is back to %d. %s", s.effectiveReplicas(), msg)
	return nil
}

func (s *singleReplicaRestore) sayResetFailed(err error) {
	log.Error("single replica restore: failed to put the replica count back",
		zap.Int("replicas", s.replicas), zap.Error(err))
	s.say("The cluster may still keep one replica of each region. Set max-replicas to %d in PD%s by hand.",
		s.replicas, s.ruleHint())
}

func (s *singleReplicaRestore) ruleHint() string {
	if s.rulesEnabled {
		return fmt.Sprintf(" and the count of the default placement rule to %d", s.defaultRule.Count)
	}
	return ""
}

func (s *singleReplicaRestore) say(format string, args ...any) {
	if s.console != nil {
		_, _ = fmt.Fprintf(s.console, format+"\n", args...)
	}
}
