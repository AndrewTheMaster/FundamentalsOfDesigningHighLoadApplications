package raftadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"lsmdb/pkg/config"
	"lsmdb/pkg/store"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type iStoreAPI interface {
	PutString(key, value string) error
	GetString(key string) (string, bool, error)
	Delete(key string) error
}

type iTransport interface {
	Send(msg raftpb.Message) error
	AddPeer(id uint64, addr string)
	RemovePeer(id uint64)
	UpdatePeer(id uint64, addr string)
}

type Node struct {
	ID           uint64
	Peers        map[uint64]string
	underlying   raft.Node
	store        iStoreAPI
	jr           *raft.MemoryStorage
	conf         *raftpb.ConfState
	tickInterval time.Duration
	transport    iTransport
	applyFilter  func(key string) bool

	ctx  context.Context
	stop context.CancelFunc

	proposalsMu sync.RWMutex
	proposals   map[uuid.UUID]chan proposeResult
}

func NewNode(config *config.RaftConfig, store iStoreAPI) (*Node, error) {
	cfg := toRaftConfig(config)
	storage := raft.NewMemoryStorage()
	cfg.Storage = storage

	var (
		confState raftpb.ConfState
		peers     = make(map[uint64]string, len(config.Peers))
		raftPeers = make([]raft.Peer, 0, len(config.Peers))
	)
	for _, p := range config.Peers {
		if _, ok := peers[p.ID]; ok {
			return nil, fmt.Errorf("duplicate peer ID %d", p.ID)
		}
		peers[p.ID] = p.Address
		confState.Voters = append(confState.Voters, p.ID)
		raftPeers = append(raftPeers, raft.Peer{
			ID:      p.ID,
			Context: []byte(p.Address),
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Node{
		ID:           config.ID,
		Peers:        peers,
		conf:         &confState,
		underlying:   raft.StartNode(cfg, raftPeers),
		store:        store,
		jr:           storage,
		tickInterval: 100 * time.Millisecond,
		transport:    NewTransport(peers),
		proposals:    make(map[uuid.UUID]chan proposeResult),
		ctx:          ctx,
		stop:         cancel,
	}, nil
}

func (n *Node) Run(ctx context.Context) error {
	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return n.ctx.Err()
		case <-ctx.Done():
			_ = n.Stop()
			return ctx.Err()
		case <-ticker.C:
			n.underlying.Tick()
		case rd := <-n.underlying.Ready():
			if err := n.handleReady(rd); err != nil {
				return err
			}
		}
	}
}

func (n *Node) handleReady(rd raft.Ready) error {
	if err := n.jr.Append(rd.Entries); err != nil {
		return fmt.Errorf("append entries: %w", err)
	}

	n.sendMessages(rd.Messages)

	for _, entry := range rd.CommittedEntries {
		if err := n.applyEntry(entry); err != nil {
			slog.Error("critical: failed to apply entry", "error", err)
			return fmt.Errorf("apply entry: %w", err)
		}

		if entry.Type == raftpb.EntryConfChange {
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				return fmt.Errorf("unmarshal conf change: %w", err)
			}
			n.conf = n.underlying.ApplyConfChange(cc)
			n.updateTransport(cc)
		}
	}

	n.underlying.Advance()
	return nil
}

func (n *Node) updateTransport(cc raftpb.ConfChange) {
	// Обновляем транспорт
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		// Добавляем новый пир
		peerAddr := string(cc.Context) // адрес из Context
		n.Peers[cc.NodeID] = peerAddr
		n.transport.AddPeer(cc.NodeID, peerAddr)
		slog.Info("added peer", "id", cc.NodeID, "addr", peerAddr)

	case raftpb.ConfChangeRemoveNode:
		// Удаляем пир
		delete(n.Peers, cc.NodeID)
		n.transport.RemovePeer(cc.NodeID)
		slog.Info("removed peer", "id", cc.NodeID)

	case raftpb.ConfChangeUpdateNode:
		// Обновляем адрес
		peerAddr := string(cc.Context)
		n.Peers[cc.NodeID] = peerAddr
		n.transport.UpdatePeer(cc.NodeID, peerAddr)
		slog.Info("updated peer", "id", cc.NodeID, "addr", peerAddr)
	}
}

func (n *Node) sendMessages(msgs []raftpb.Message) {
	for _, msg := range msgs {
		if msg.To == n.ID {
			continue
		}

		go func(m raftpb.Message) {
			if err := n.transport.Send(m); err != nil {
				slog.Error("failed to send raft message",
					"from", m.From,
					"to", m.To,
					"type", m.Type,
					"error", err)
			}
		}(msg)
	}
}

func (n *Node) applyEntry(entry raftpb.Entry) error {
	if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
		return nil
	}

	var cmd Cmd
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshal command: %w", err)
	}

	key := string(cmd.Key)

	// изменения из-за шардирование+репликация
	// Нода применяет запись, только если она "реплика" для этого ключа.
	if n.applyFilter != nil && !n.applyFilter(key) {
		// важно: для лидера нужно всё равно разбудить ожидание результата
		return n.notifyProposalResult(cmd.ID, proposeResult{Err: nil})
	}

	var err error
	switch cmd.Op {
	case store.InsertOp:
		err = n.store.PutString(string(cmd.Key), string(cmd.Value))
	case store.DeleteOp:
		err = n.store.Delete(string(cmd.Key))
	default:
		err = fmt.Errorf("unknown command operation: %v", cmd.Op)
	}

	return n.notifyProposalResult(cmd.ID, proposeResult{Err: err})
}

func (n *Node) IsLeader() bool {
	return n.underlying.Status().Lead == n.ID
}

func (n *Node) LeaderAddr() string {
	leaderID := n.underlying.Status().Lead
	return n.Peers[leaderID]
}

type proposeResult struct {
	Err error
}

func (n *Node) notifyProposalResult(cmdID uuid.UUID, result proposeResult) error {
	n.proposalsMu.RLock()
	resultChan, ok := n.proposals[cmdID]
	n.proposalsMu.RUnlock()

	if !ok {
		// - follower применяет запись (у него не было proposals[cmdID])
		// - лидерский Execute уже завершился (timeout/cancel), defer удалил proposals[cmdID]
		// - лидер сменился, а apply пришёл позже
		slog.Debug("proposal result channel not found (ignored)", "cmd_id", cmdID, "is_leader", n.IsLeader())
		return nil
	}

	// не блокируем apply, если вдруг слушатель уже ушёл
	select {
	case resultChan <- result:
	default:
		slog.Debug("proposal result channel is full (ignored)", "cmd_id", cmdID)
	}
	return nil
}

func (n *Node) validateCommand(cmd Cmd) error {
	switch cmd.Op {
	case store.InsertOp:
		if len(cmd.Key) == 0 || len(cmd.Value) == 0 {
			return fmt.Errorf("invalid command: empty key or value")
		}
	case store.DeleteOp:
		if len(cmd.Key) == 0 {
			return fmt.Errorf("invalid command: empty key")
		}
	default:
		return fmt.Errorf("unknown operation: %v", cmd.Op)
	}
	return nil
}

func (n *Node) Execute(ctx context.Context, cmd Cmd) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	if err := n.validateCommand(cmd); err != nil {
		return err
	}
	resultChan := make(chan proposeResult, 1)

	n.proposalsMu.Lock()
	n.proposals[cmd.ID] = resultChan
	n.proposalsMu.Unlock()

	defer func() {
		n.proposalsMu.Lock()
		delete(n.proposals, cmd.ID)
		n.proposalsMu.Unlock()
	}()

	if err := n.underlying.Propose(ctx, data); err != nil {
		return fmt.Errorf("propose: %w", err)
	}

	select {
	case result := <-resultChan:
		return result.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Handle обрабатывает входящие Raft-сообщения от других нод
func (n *Node) Handle(ctx context.Context, msg raftpb.Message) error {
	return n.underlying.Step(ctx, msg)
}

func (n *Node) Stop() error {
	slog.Info("stopping raft node", "id", n.ID)

	n.underlying.Stop()
	n.stop()

	n.proposalsMu.Lock()
	for _, resultChan := range n.proposals {
		select {
		case resultChan <- proposeResult{Err: fmt.Errorf("node stopped")}:
		default:
		}
		close(resultChan)
	}
	n.proposalsMu.Unlock()

	slog.Info("raft node stopped", "id", n.ID)
	return nil
}

// SetApplyFilter задаёт фильтр: применять команду к local store или пропускать.
// Если фильтр nil — применяем всё (как раньше).
func (n *Node) SetApplyFilter(fn func(key string) bool) {
	n.applyFilter = fn
}

func (n *Node) LeaderID() uint64 {
	return n.underlying.Status().Lead
}
