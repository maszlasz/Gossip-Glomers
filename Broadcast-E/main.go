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
	node *maelstrom.Node
	// set of received values
	values   map[float64]struct{}
	valuesMx sync.Mutex
	// buffer for resending values, src -> values,
	// so as to not pollute the nw with duplicates
	valuesBuf   map[string][]float64
	valuesBufMx sync.Mutex
	neighbors   []string
}

const centralNode = "n0"

func main() {
	s := state{
		node:      maelstrom.NewNode(),
		values:    make(map[float64]struct{}),
		valuesBuf: make(map[string][]float64)}

	s.node.Handle("broadcast", s.handleBroadcast)
	s.node.Handle("read", s.handleRead)
	s.node.Handle("topology", s.handleTopology)

	go s.broadcastLoop()

	if err := s.node.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *state) handleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var messages []any
	var values []float64

	s.valuesMx.Lock()
	if message, ok := body["message"]; ok {
		messages = append(messages, message)
	} else {
		messages = body["messages"].([]any)
	}

	for _, message := range messages {
		value := message.(float64)
		if _, ok := s.values[value]; !ok {
			s.values[value] = struct{}{}
			values = append(values, value)
		}
	}
	s.valuesMx.Unlock()

	s.valuesBufMx.Lock()
	src := msg.Src
	if valuesBuf, ok := s.valuesBuf[src]; ok {
		s.valuesBuf[src] = append(valuesBuf, values...)
	} else {
		s.valuesBuf[src] = values
	}
	s.valuesBufMx.Unlock()

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

func (s *state) broadcastLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		<-ticker.C

		s.valuesBufMx.Lock()
		if len(s.valuesBuf) > 0 {
			valuesBufTmp := make(map[string][]float64)
			for src, values := range s.valuesBuf {
				valuesTmp := make([]float64, len(values))
				copy(valuesTmp, values)
				valuesBufTmp[src] = valuesTmp
			}

			s.valuesBuf = make(map[string][]float64)
			s.valuesBufMx.Unlock()

			for _, id := range s.neighbors {
				go s.broadcast(id, valuesBufTmp)
			}
		} else {
			s.valuesBufMx.Unlock()
		}
	}
}

func (s *state) broadcast(dst string, valuesFromSrcs map[string][]float64) {
	messages := []float64{}
	for src, values := range valuesFromSrcs {
		if dst != src {
			messages = append(messages, values...)
		}
	}

	if len(messages) == 0 {
		return
	}

	body := map[string]any{
		"type":     "broadcast",
		"messages": messages}

	mult := 250
	for {
		timeLimit :=
			time.Now().Add(time.Duration(mult) * time.Millisecond)
		ctx, cancel :=
			context.WithDeadline(context.Background(), timeLimit)
		defer cancel()
		if _, err := s.node.SyncRPC(ctx, dst, body); err == nil {
			break
		}
		mult *= 2
	}
}

// rwmutex maybe?