#!/bin/bash

go build -o solution
../maelstrom/maelstrom test -w unique-ids --bin ./solution --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition