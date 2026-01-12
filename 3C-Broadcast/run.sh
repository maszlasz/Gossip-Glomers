#!/bin/bash

go build -o solution
../maelstrom/maelstrom test -w broadcast --bin ./solution --node-count 5 --time-limit 20 --rate 10 --nemesis partition