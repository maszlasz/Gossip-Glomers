package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type state struct {
	node    *maelstrom.Node
	kv      *maelstrom.KV
	cache   map[string][]any
	cacheMx sync.Mutex
}

func main() {
	s := state{
		node:  maelstrom.NewNode(),
		cache: make(map[string][]any)}
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
	keyNum, _ := strconv.Atoi(key)
	id := keyNum % len(s.node.NodeIDs())

	ownId, _ := strconv.Atoi(s.node.ID()[1:])

	var offset int
	if id == ownId {
		val := body["msg"].(float64)

		s.cacheMx.Lock()
		var valsNew []any
		valsOld, present := s.cache[key]
		if present {
			valsNew = append(valsOld, val)
		} else {
			valsNew = []any{val}
		}
		s.cache[key] = valsNew
		s.cacheMx.Unlock()

		err := s.kv.Write(context.Background(), key, valsNew)

		if err != nil {
			log.Println(err.Error())
		}

		offset = len(valsOld)
	} else {
		idStr := strconv.Itoa(id)
		dst := "n" + idStr

		if res, err := s.node.SyncRPC(context.Background(), dst, msg.Body); err == nil {
			var body map[string]any
			if err := json.Unmarshal(res.Body, &body); err != nil {
				return err
			}
			offset = int(body["offset"].(float64))
		} else {
			log.Println(err.Error())
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

		valsRaw, err := s.kv.Read(context.Background(), key)

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
		offset := int(offsetRaw.(float64))

		err := s.kv.Write(context.Background(), key+"_commited", offset)

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
		offsetRaw, err := s.kv.Read(context.Background(), key+"_commited")

		if err == nil {
			offset := offsetRaw.(int)
			offsets[key] = offset
		} else if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			log.Println(err.Error())
		}
	}

	replyBody := map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets}
	return s.node.Reply(msg, replyBody)
}
