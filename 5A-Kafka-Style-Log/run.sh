#!/bin/bash

go build -o solution
../maelstrom/maelstrom test -w kafka --bin ./solution --node-count 1 --concurrency 2n --time-limit 20 --rate 1000