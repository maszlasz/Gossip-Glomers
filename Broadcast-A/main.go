package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	var vals []float64

	n.Handle(
		"broadcast",
		func(msg maelstrom.Message) error {
			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
			}

			vals = append(vals, body["message"].(float64))
			delete(body, "message")
			body["type"] = "broadcast_ok"

			return n.Reply(msg, body)
		})

	n.Handle(
		"read",
		func(msg maelstrom.Message) error {
			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
			}

			body["type"] = "read_ok"
			body["messages"] = vals

			return n.Reply(msg, body)
		})

	n.Handle(
		"topology",
		func(msg maelstrom.Message) error {
			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
			}

			body["type"] = "topology_ok"
			delete(body, "topology")

			return n.Reply(msg, body)
		})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
