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
					for {
						if _, err := s.node.SyncRPC(ctx, id, body); err == nil {
							ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(100*time.Millisecond))
							defer cancel()
							break
						}
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

	ids := body["topology"].(map[string]any)
	ownId := s.node.ID()

	neighborsRaw := ids[ownId].([]any)
	s.neighbors = make([]string, 0, len(neighborsRaw))
	for _, node := range neighborsRaw {
		s.neighbors = append(s.neighbors, node.(string))
	}

	replyBody := map[string]any{
		"type": "topology_ok"}
	return s.node.Reply(msg, replyBody)
}
