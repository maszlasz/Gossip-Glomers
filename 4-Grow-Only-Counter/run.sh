#!/bin/bash

go build -o solution
../maelstrom/maelstrom test -w g-counter --bin ./solution --node-count 3 --rate 100 --time-limit 20 --nemesis partition