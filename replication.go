package raft

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/armon/go-metrics"
)

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

var (
	// ErrLogNotFound indicates a given log entry is not available.
	ErrLogNotFound = errors.New("log not found")

	// ErrPipelineReplicationNotSupported can be returned by the transport to
	// signal that pipeline replication is not supported in general, and that
	// no error message should be produced.
	ErrPipelineReplicationNotSupported = errors.New("pipeline replication not supported")
)

type replicationControl struct {
	// If shutdown is true, the goroutine should try to replicate up to
	// shutdownIndex, then shut down.
	shutdown bool

	// If shutdown is true, the goroutine should try to replicate up to
	// this log index, then shut down.
	shutdownIndex uint64

	// Incremented whenever replication should send a ping immediately.
	verifyTerm uint64
}

type replicationProgress struct {
	term uint64

	matchIndex uint64

	// lastContact is updated to the current time whenever any response is
	// received from the follower (successful or not). This is used to check
	// whether the leader should step down (Raft.checkLeaderLease()).
	lastContact time.Time
}

type doneRPC struct {
	start time.Time
	req   interface{}
	resp  interface{}
	err   error
}

// This struct is private to this replication goroutine (leaderLoop shouldn't reach in here).
type replicationPrivate struct {
	lastControl replicationControl

	// allowPipeline is used to determine when to pipeline the AppendEntries RPCs.
	allowPipeline bool

	// failures counts the number of failed RPCs since the last success, which is
	// used to apply backoff.
	failures uint64

	// Set iff failures > 0.
	backoffTimer time.Timer

	matchIndex uint64
	// nextIndex is the index of the next log entry to send to the follower,
	// which may fall past the end of the log.
	nextIndex uint64

	doneRPCs chan doneRPC

	pipeline         AppendPipeline
	pipelineStopCh   chan struct{}
	pipelineFinishCh chan struct{}

	stopHeartbeat chan struct{}
}

type replicationLeader struct {
}

type replication struct {
	logger *log.Logger

	raft *Raft

	// peer contains the network address and ID of the remote follower (constant).
	peer Server

	// The current term of the leader (constant).
	term uint64

	// Protects 'control'.
	controlLock sync.Mutex

	// Notified whenever the log is extended, the commitIndex is updated, or
	// any of the 'control' fields change.
	controlCh chan struct{}

	control replicationControl

	// Protects 'progress'.
	progressLock sync.Mutex
	// Notified whenever any of the fields in 'progress' change.
	progressCh chan struct{}
	progress   replicationProgress

	private replicationPrivate
	leader  replicationLeader
}

func newReplication(raft *Raft, peer Server, term uint64) *replication {
	var repl = &replication{
		logger: raft.logger,
		raft:   raft,
		peer:   peer,
		term:   term,
		control: replicationControl{
			verifyTerm: 1,
		},
	}
	repl.controlCh = make(chan struct{}, 1)
	repl.progressCh = make(chan struct{}, 1)
	repl.private.doneRPCs = make(chan doneRPC, 8)

	pipeline, err := raft.trans.AppendEntriesPipeline(peer.Address)
	if err != nil {
		if err != ErrPipelineReplicationNotSupported {
			repl.logger.Printf("[ERR] raft: Failed to start pipeline replication to %s: %s", peer, err)
		}
	} else {
		repl.private.pipeline = pipeline
		repl.private.pipelineStopCh = make(chan struct{})
		repl.private.pipelineFinishCh = make(chan struct{})
		raft.goFunc(func() { repl.drainPipelineReplies() })
	}

	repl.private.stopHeartbeat = make(chan struct{})

	raft.goFunc(func() { repl.replicate() })
	raft.goFunc(func() { repl.heartbeat() })

	return repl
}

func (repl *replication) stop() {

	close(repl.private.stopHeartbeat)

	// TODO: why bother?
	repl.private.pipeline.Close()
	close(repl.private.pipelineStopCh)

	for {
		select {
		case <-repl.private.pipelineFinishCh:
			return
		case <-repl.private.doneRPCs:
		}
	}
}

// replicate is a long running routine that replicates log entries to a single
// follower.
func (repl *replication) replicate() {

	for {
		// Nonblocking loop: read any new control information and process all
		// completed RPCs.
		for more := true; more; {
			select {
			case <-repl.controlCh:
				repl.controlLock.Lock()
				repl.private.lastControl = repl.control
				repl.controlLock.Unlock()
			case doneRPC := <-repl.private.doneRPCs:
				repl.processRPC(doneRPC)
			default:
				more = false
			}
		}

		if repl.private.lastControl.shutdown {
			break
		}

		// Send an RPC if we can.
		if repl.private.matchIndex < repl.raft.LastIndex() &&
			repl.private.failures == 0 {
			repl.sendAppendEntries()
			continue
		}

		// Block until something changes.
		select {
		case <-repl.controlCh:
			repl.controlLock.Lock()
			repl.private.lastControl = repl.control
			repl.controlLock.Unlock()
		case doneRPC := <-repl.private.doneRPCs:
			repl.processRPC(doneRPC)
		case <-repl.private.backoffTimer.C:
			repl.private.failures = 0
		}

		if repl.private.lastControl.shutdown {
			break
		}
	}
}

func (repl *replication) processRPC(rpc doneRPC) {
	if rpc.err != nil {
		repl.private.failures++
		repl.private.backoffTimer.Reset(backoff(failureWait, repl.private.failures, maxFailureScale))
		return
	}

	repl.progressLock.Lock()
	defer repl.progressLock.Unlock()
	asyncNotifyCh(repl.progressCh)

	// Process completed AppendEntries.
	if req, ok := rpc.req.(AppendEntriesRequest); ok {
		resp := rpc.resp.(AppendEntriesResponse)
		repl.progress.term = resp.Term
		repl.progress.lastContact = time.Now() // TODO: rpc.start?
		if resp.Term == req.Term {
			if resp.Success {
				lastIndex := req.PrevLogEntry + uint64(len(req.Entries))
				repl.progress.matchIndex = lastIndex
				repl.private.matchIndex = lastIndex
				repl.private.nextIndex = lastIndex + 1
				repl.private.failures = 0
				repl.private.backoffTimer.Stop()
				repl.private.allowPipeline = true
			} else {
				if repl.private.nextIndex > 1 {
					// TODO: update for pipelining
					repl.private.nextIndex = min(repl.private.nextIndex-1, resp.LastLog+1)
				}
				if resp.NoRetryBackoff {
					repl.private.failures = 0
					repl.private.backoffTimer.Stop()
				} else {
					repl.private.failures++
					repl.private.backoffTimer.Reset(backoff(failureWait, repl.private.failures, maxFailureScale))
				}
				repl.logger.Printf("[WARN] raft: AppendEntries to %v rejected, sending older log entries (next: %d)", repl.peer, repl.private.nextIndex)
			}
		}
		return
	}

	// Process completed InstallSnapshot.
	if req, ok := rpc.req.(InstallSnapshotRequest); ok {
		resp := rpc.resp.(InstallSnapshotResponse)
		metrics.MeasureSince([]string{"raft", "replication", "installSnapshot", string(repl.peer.ID)}, rpc.start)
		repl.progress.term = resp.Term
		repl.progress.lastContact = time.Now() // TODO: rpc.start?
		if resp.Term == req.Term {
			if resp.Success {
				repl.private.matchIndex = req.LastLogIndex
				repl.private.nextIndex = req.LastLogIndex + 1
				repl.private.failures = 0
				repl.private.backoffTimer.Stop()
			} else {
				repl.private.failures++
				repl.private.backoffTimer.Reset(backoff(failureWait, repl.private.failures, maxFailureScale))
			}
			repl.logger.Printf("[WARN] raft: InstallSnapshot to %v rejected", repl.peer)
		}
		return
	}

	repl.logger.Fatalf("Unknown RPC type: %v", rpc)
}

// TODO: update doc
// replicateTo is a hepler to replicate(), used to replicate the logs up to a
// given last index.
// If the follower log is behind, we take care to bring them up to date.
func (repl *replication) sendAppendEntries() {
	// Create the base request
	var req AppendEntriesRequest
	var resp AppendEntriesResponse
	// Setup the request
	if err := repl.setupAppendEntries(&req); err == ErrLogNotFound {
		repl.sendSnapshot()
	} else if err != nil {
		// TODO: why might this happen, and should it have backoff?
		repl.logger.Printf("[ERR] raft: Failed to setup AppendEntries for %v: %v", repl.peer, err)
		return
	}

	if repl.private.allowPipeline && repl.private.pipeline != nil {
		// Replicates using a pipeline for high performance. This method
		// is not able to gracefully recover from errors, and so we fall back
		// to standard mode on failure.
		// TODO: what does ^ mean?
		if _, err := repl.private.pipeline.AppendEntries(&req, &resp); err != nil {
			repl.logger.Printf("[ERR] raft: Failed to pipeline AppendEntries to %v: %v", repl.peer, err)
			repl.private.allowPipeline = false
		}
		// TODO: something about nextIndex
	} else {
		// Make the RPC call
		start := time.Now()
		err := repl.raft.trans.AppendEntries(repl.peer.Address, &req, &resp)
		if err != nil {
			repl.logger.Printf("[ERR] raft: Failed to send AppendEntries to %v: %v", repl.peer, err)
		}
		appendStats(repl.peer.ID, start, len(req.Entries))
		repl.private.doneRPCs <- doneRPC{
			start: start,
			req:   req,
			resp:  resp,
			err:   err,
		}
	}
}

// sendSnapshot is used to send the latest snapshot we have
// down to our follower.
func (repl *replication) sendSnapshot() {
	// Get the snapshots
	snapshots, err := repl.raft.snapshots.List()
	if err != nil {
		// TODO: panic/backoff?
		repl.logger.Printf("[ERR] raft: Failed to list snapshots: %v", err)
		return
	}

	// Check we have at least a single snapshot
	if len(snapshots) == 0 {
		// TODO: panic/backoff?
		repl.logger.Printf("[ERR] raft: Sending snapshot but no snapshots found")
		return
	}

	// Open the most recent snapshot
	snapID := snapshots[0].ID
	meta, snapshot, err := repl.raft.snapshots.Open(snapID)
	if err != nil {
		// TODO: panic/backoff?
		repl.logger.Printf("[ERR] raft: Failed to open snapshot %v: %v", snapID, err)
		return
	}
	defer snapshot.Close()

	// Setup the request
	req := InstallSnapshotRequest{
		Term:               repl.term,
		Leader:             repl.raft.trans.EncodePeer(repl.raft.localAddr),
		LastLogIndex:       meta.Index,
		LastLogTerm:        meta.Term,
		Peers:              meta.Peers,
		Size:               meta.Size,
		Configuration:      encodeConfiguration(meta.Configuration),
		ConfigurationIndex: meta.ConfigurationIndex,
	}

	// Make the call
	start := time.Now()
	var resp InstallSnapshotResponse
	err = repl.raft.trans.InstallSnapshot(repl.peer.Address, &req, &resp, snapshot)
	if err != nil {
		repl.logger.Printf("[ERR] raft: Failed to install snapshot %v: %v", snapID, err)
	}
	repl.private.doneRPCs <- doneRPC{
		start: start,
		req:   req,
		resp:  resp,
		err:   err,
	}
}

// heartbeat is used to periodically invoke AppendEntries on a peer
// to ensure they don't time out. This is done async of replicate(),
// since that routine could potentially be blocked on disk IO.
func (repl *replication) heartbeat() {
	/*
		var failures uint64
		req := AppendEntriesRequest{
			Term:   repl.term,
			Leader: repl.raft.trans.EncodePeer(r.localAddr),
		}
		var resp AppendEntriesResponse
		for {
			// Wait for the next heartbeat interval or forced notify
			select {
			case <-s.notifyCh:
			case <-randomTimeout(r.conf.HeartbeatTimeout / 10):
			case <-repl.private.stopHeartbeat:
				return
			}

			start := time.Now()
			if err := r.trans.AppendEntries(s.peer.Address, &req, &resp); err != nil {
				r.logger.Printf("[ERR] raft: Failed to heartbeat to %v: %v", s.peer.Address, err)
				failures++
				select {
				case <-time.After(backoff(failureWait, failures, maxFailureScale)):
				case <-stopCh:
				}
			} else {
				s.setLastContact()
				failures = 0
				metrics.MeasureSince([]string{"raft", "replication", "heartbeat", string(s.peer.ID)}, start)
				s.notifyAll(resp.Success)
			}
		}
	*/
}

// TODO: update/delete doc
// pipelineReplicate is used when we have synchronized our state with the follower,
// and want to switch to a higher performance pipeline mode of replication.
// We only pipeline AppendEntries commands, and if we ever hit an error, we fall
// back to the standard replication which can handle more complex situations.

/*
	// Log start and stop of pipeline
	r.logger.Printf("[INFO] raft: pipelining replication to peer %v", s.peer)
	defer r.logger.Printf("[INFO] raft: aborting pipeline replication to peer %v", s.peer)
*/

func (repl *replication) drainPipelineReplies() {
	defer close(repl.private.pipelineFinishCh)
	respCh := repl.private.pipeline.Consumer()
	for {
		select {
		case ready := <-respCh:
			req, resp := ready.Request(), ready.Response()
			appendStats(repl.peer.ID, ready.Start(), len(req.Entries))
			repl.private.doneRPCs <- doneRPC{
				start: ready.Start(),
				req:   req,
				resp:  resp,
				err:   nil,
			}
		case <-repl.private.pipelineStopCh:
			return
		}
	}
}

// setupAppendEntries is used to create an AppendEntries request.
func (repl *replication) setupAppendEntries(req *AppendEntriesRequest) error {
	req.Term = repl.term
	req.Leader = repl.raft.trans.EncodePeer(repl.raft.localAddr)
	req.LeaderCommitIndex = repl.raft.getCommitIndex()
	req.PrevLogEntry = repl.private.nextIndex - 1
	term, err := repl.raft.getTerm(req.PrevLogEntry)
	if err != nil {
		return err
	}
	req.PrevLogTerm = term

	// Append up to MaxAppendEntries or up to ... TODO
	lastIndex := min(min(repl.raft.getLastIndex(), repl.private.lastControl.shutdownIndex),
		req.PrevLogEntry+uint64(repl.raft.conf.MaxAppendEntries))
	req.Entries = make([]*Log, 0, repl.raft.conf.MaxAppendEntries)
	for i := req.PrevLogEntry + 1; i <= lastIndex; i++ {
		oldLog := new(Log)
		if err := repl.raft.logs.GetLog(i, oldLog); err != nil {
			repl.logger.Printf("[ERR] raft: Failed to get log at index %d: %v", i, err)
			return err
		}
		req.Entries = append(req.Entries, oldLog)
	}
	return nil
}

// appendStats is used to emit stats about an AppendEntries invocation.
func appendStats(peer ServerID, start time.Time, logs int) {
	metrics.MeasureSince([]string{"raft", "replication", "appendEntries", "rpc", string(peer)}, start)
	metrics.IncrCounter([]string{"raft", "replication", "appendEntries", "logs", string(peer)}, float32(logs))
}

//repl.logger.Printf("[ERR] raft: peer %v has newer term, stopping replication", s.peer)
