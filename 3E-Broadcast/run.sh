#!/bin/bash

go build -o solution
../maelstrom/maelstrom test -w broadcast --bin ./solution --node-count 25 --time-limit 20 --rate 100 --latency 100