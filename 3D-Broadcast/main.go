package main

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type state struct {
	node      *maelstrom.Node
	values    map[float64]struct{}
	valuesMx  sync.Mutex
	neighbors []string
}

const centralNode = "n0"

func main() {
	s := state{
		node:   maelstrom.NewNode(),
		values: make(map[float64]struct{})}

	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("topology", s.handleTopology)

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *state) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.valuesMx.Lock()
	value := body["message"].(float64)
	if _, ok := s.values[value]; !ok {
		s.values[value] = struct{}{}
		s.valuesMx.Unlock()

		for _, id := range s.neighbors {
			if id != msg.Src {
				go func() {
					mult := 250
					for {
						timeLimit :=
							time.Now().Add(time.Duration(mult) * time.Millisecond)
						ctx, cancel :=
							context.WithDeadline(context.Background(), timeLimit)
						defer cancel()
						if _, err := s.node.SyncRPC(ctx, id, body); err == nil {
							break
						}
						mult *= 2
					}
				}()
			}
		}
	} else {
		s.valuesMx.Unlock()
	}

	replyBody := map[string]any{
		"type": "broadcast_ok"}
	return s.node.Reply(msg, replyBody)
}

func (s *state) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.valuesMx.Lock()
	values := slices.Collect(maps.Keys(s.values))
	s.valuesMx.Unlock()

	replyBody := map[string]any{
		"type":     "read_ok",
		"messages": values}
	return s.node.Reply(msg, replyBody)
}

func (s *state) handleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Ignore the given topology, assume star-like
	if s.node.ID() == centralNode {
		s.neighbors = s.node.NodeIDs()
		s.neighbors =
			slices.DeleteFunc(s.neighbors, func(ele string) bool {
				return ele == centralNode
			})
	} else {
		s.neighbors = []string{centralNode}
	}

	replyBody := map[string]any{
		"type": "topology_ok"}
	return s.node.Reply(msg, replyBody)
}
