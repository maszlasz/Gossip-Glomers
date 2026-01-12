package main

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type state struct {
	node                *maelstrom.Node
	logs                map[string][]float64
	committed_offsets   map[string]int
	logsMx              sync.Mutex
	committed_offsetsMx sync.Mutex
}

func main() {
	s := state{
		node:              maelstrom.NewNode(),
		logs:              make(map[string][]float64),
		committed_offsets: make(map[string]int)}

	s.node.Handle("send", s.handleSend)
	s.node.Handle("poll", s.handlePoll)
	s.node.Handle("commit_offsets", s.handleCommitOffsets)
	s.node.Handle("list_committed_offsets", s.handleListCommittedOffsets)

	if err := s.node.Run(); err != nil {
		panic(err)
	}
}

func (s *state) handleSend(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	key := body["key"].(string)
	val := body["msg"].(float64)

	var offset int
	s.logsMx.Lock()
	if vals, present := s.logs[key]; present {
		offset = len(vals)
		s.logs[key] = append(vals, val)
	} else {
		offset = 0
		s.logs[key] = []float64{val}
	}
	s.logsMx.Unlock()

	replyBody := map[string]any{
		"type":   "send_ok",
		"offset": offset}
	return s.node.Reply(msg, replyBody)
}

func (s *state) handlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offsets := body["offsets"].(map[string]any)

	msgs := make(map[string][][]float64)
	s.logsMx.Lock()
	for key, offsetRaw := range offsets {
		offset := int(offsetRaw.(float64))
		keyMsgs := s.logs[key][offset:len(s.logs[key])]

		offsetMsgPairs := [][]float64{}
		for index, msg := range keyMsgs {
			realOffset := float64(offset + index)
			offsetMsgPairs = append(offsetMsgPairs, []float64{realOffset, msg})
		}

		msgs[key] = offsetMsgPairs
	}
	s.logsMx.Unlock()

	replyBody := map[string]any{
		"type": "poll_ok",
		"msgs": msgs}
	return s.node.Reply(msg, replyBody)
}

func (s *state) handleCommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	offsets := body["offsets"].(map[string]any)

	s.committed_offsetsMx.Lock()
	for key, offsetRaw := range offsets {
		offset := int(offsetRaw.(float64))
		s.committed_offsets[key] = offset
	}
	s.committed_offsetsMx.Unlock()

	replyBody := map[string]any{
		"type": "commit_offsets_ok"}
	return s.node.Reply(msg, replyBody)
}

func (s *state) handleListCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	keys := body["keys"].([]any)

	offsets := make(map[string]int)

	s.committed_offsetsMx.Lock()
	for _, keyRaw := range keys {
		key := keyRaw.(string)
		offsets[key] = s.committed_offsets[key]
	}
	s.committed_offsetsMx.Unlock()

	replyBody := map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets}
	return s.node.Reply(msg, replyBody)
}
