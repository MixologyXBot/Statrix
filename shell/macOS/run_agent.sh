#!/bin/bash
# Wrapper: launch agent in background and exit immediately
# AbandonProcessGroup in the plist ensures the child survives
/bin/bash /opt/statrix/statrix_agent.sh &
