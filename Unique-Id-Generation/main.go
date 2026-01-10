package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	n.Handle(
		"generate",
		func(msg maelstrom.Message) error {
			var body map[string]any
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				return err
			}

			body["type"] = "generate_ok"
			body["id"] = getUUID()

			return n.Reply(msg, body)
		})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

const (
	randMax1 = 1 << 12
	randMax2 = 1 << 14
	randMax3 = 1 << 48
)

func getUUID() string {
	now := time.Now().UnixMilli()
	now1 := now >> 16
	now2 := now & 0xffff

	rand1 := rand.IntN(randMax1)
	version := 0x7
	rand1 = version<<12 + rand1

	rand2 := rand.IntN(randMax2)
	variant := 0b10
	rand2 = variant<<14 + rand2

	rand3 := rand.IntN(randMax3)

	uuid := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", now1, now2, rand1, rand2, rand3)
	return uuid
}
