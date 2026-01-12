#!/bin/bash

go build -o solution
../maelstrom/maelstrom test -w echo --bin ./solution --node-count 1 --time-limit 10