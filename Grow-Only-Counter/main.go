package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type state struct {
	node           *maelstrom.Node
	kv             *maelstrom.KV
	global_counter int
	own_counter    int
}

func main() {
	s := state{
		node: maelstrom.NewNode()}
	s.kv = maelstrom.NewSeqKV(s.node)

	s.node.Handle("read", s.handleRead)
	s.node.Handle("add", s.handleAdd)

	if err := s.node.Run(); err != nil {
		panic(err)
	}
}

func (s *state) handleRead(msg maelstrom.Message) error {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()
		err := s.kv.Write(ctx, s.node.ID(), s.own_counter)
		if err != nil {
			log.Println(err.Error())
		}
		s.own_counter += 1

		ctx, cancel = context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()
		global_counter, err := s.kv.Read(ctx, "counter")

		if err == nil {
			s.global_counter = global_counter.(int)
			break
		} else if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			log.Println(err.Error())
			s.global_counter = 0
			break
		} else {
			log.Println(err.Error())
		}
	}

	replyBody := map[string]any{
		"type":  "read_ok",
		"value": s.global_counter}
	return s.node.Reply(msg, replyBody)
}

func (s *state) handleAdd(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	delta := int(body["delta"].(float64))

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		defer cancel()
		err := s.kv.CompareAndSwap(ctx, "counter", s.global_counter, s.global_counter+delta, true)
		if err != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
			defer cancel()
			global_counter, err := s.kv.Read(ctx, "counter")

			if err != nil {
				log.Println(err)
			}

			s.global_counter = global_counter.(int)
		} else {
			break
		}
	}

	replyBody := map[string]any{
		"type": "add_ok"}
	return s.node.Reply(msg, replyBody)
}
