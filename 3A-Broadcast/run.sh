#!/bin/bash

go build -o solution
../maelstrom/maelstrom test -w broadcast --bin ./solution --node-count 1 --time-limit 20 --rate 10