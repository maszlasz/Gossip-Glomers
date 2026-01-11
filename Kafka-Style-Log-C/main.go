package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type state struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
}

func main() {
	s := state{
		node: maelstrom.NewNode()}
	s.kv = maelstrom.NewLinKV(s.node)

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
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()
		valsRaw, err := s.kv.Read(ctx, key)

		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			valsNew := []float64{val}

			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()
			err := s.kv.CompareAndSwap(ctx, key, nil, valsNew, true)
			if err == nil {
				offset = 0
				break
			}
		} else if err == nil {
			valsOld := valsRaw.([]any)
			valsNew := append(valsOld, val)

			ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()
			err := s.kv.CompareAndSwap(ctx, key, valsOld, valsNew, false)
			if err == nil {
				offset = len(valsOld)
				break
			}
		}
	}

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
	for key, offsetRaw := range offsets {
		offset := int(offsetRaw.(float64))

		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()
		valsRaw, err := s.kv.Read(ctx, key)

		offsetMsgPairs := [][]float64{}
		if err == nil {
			vals := valsRaw.([]any)

			keyMsgs := vals[offset:]

			for index, msgRaw := range keyMsgs {
				msg := msgRaw.(float64)
				realOffset := float64(offset + index)
				offsetMsgPairs = append(offsetMsgPairs, []float64{realOffset, msg})
			}

		}
		msgs[key] = offsetMsgPairs
	}

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

	for key, offsetRaw := range offsets {
		key += "_commited"
		offset := int(offsetRaw.(float64))

		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()
		err := s.kv.Write(ctx, key, offset)

		if err != nil {
			log.Println(err.Error())
		}
	}

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

	for _, keyRaw := range keys {
		key := keyRaw.(string)

		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()
		offsetRaw, err := s.kv.Read(ctx, key+"_commited")

		if err == nil {
			offset := offsetRaw.(int)
			offsets[key] = offset
		} else if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			log.Println(err.Error())
		}
	}

	replyBody := map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets}
	return s.node.Reply(msg, replyBody)
}
