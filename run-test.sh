#!/bin/bash

echo "Delete the test database"
rm -f resource-test/test-agent.db
# Create the test database
python3 init_database.py resource-test/config_agent.json
sqlite3 resource-test/test-agent.db < resource-test/test.sql
# Start the agent_api on the port 1234
python3 agent_api.py resource-test/config_agent.json &> resource-test/agent_api.output &
sleep 1
# Run the tests
python3 run-test.py resource-test/config_agent.json
# Kill the agent_api
api_pid=$(ps aux | grep resource-test/config_agent.json | grep -v grep)
if [ ! -z "$api_pid" ]; then
    echo -n "Kill the running agent_api"
    pid=$(echo $api_pid | cut -d " " -f2)
    echo " (pid: $pid)"
    kill -9 $pid
fi
