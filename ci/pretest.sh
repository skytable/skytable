#!/bin/bash
export ORIGIN_KEY=4527387f92a381cbe804593f33991d327d456a97
function ensure_eq() {
    local q1=`${RUNSKYSH} "${1}" | tr -d '[:space:]' | sed -r 's/\x1B\[(;?[0-9]{1,3})+[mGK]//g'`
    local q1e=${2}
    if [[ "$q1" != "$q1e" ]]; then
        echo "Expected '${q1e}', but got '${q1}' instead"
        exit 1
    fi
}
OKAY="(Okay)"
RUNSKYSH="cargo run ${TARGET} -p skysh -- --port 2005 -e"
export ROOTUSER_TOKEN=`${RUNSKYSH} "auth claim ${ORIGIN_KEY}" | tr -d '[:space:]' | tr -d "\""`

# login as root
export TESTUSER_TOKEN=`${RUNSKYSH} "auth login root ${ROOTUSER_TOKEN}" -e "auth adduser testuser"\
 | head -n 2 | tail -n 1 | tr -d '[:space:]' | tr -d "\"" | sed -r 's/\x1B\[(;?[0-9]{1,3})+[mGK]//g'`
echo "TESTUSER_TOKEN=${TESTUSER_TOKEN}" >> .skytestenv
echo "ROOTUSER_TOKEN=${ROOTUSER_TOKEN}" >> .skytestenv
