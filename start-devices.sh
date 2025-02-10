#!/bin/bash
tmux has-session -t digilent-mcc134-thermocouples 2>/dev/null || tmux new-session -d -s digilent-mcc134-thermocouples 'python /Users/matt/Documents/GitHub/ida-devices/digilent-mcc134-thermocouples.py'
tmux has-session -t mks-925-vacuum 2>/dev/null || tmux new-session -d -s mks-925-vacuum 'python /Users/matt/Documents/GitHub/ida-devices/mks-925-vacuum.py'
