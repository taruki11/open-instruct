#!/bin/bash
export CURRENT_DATETIME=$(python -c "import datetime; import pytz; print(datetime.datetime.now(pytz.timezone('America/Los_Angeles')).strftime('%m%d%y_%H%M%S'))")
export PYTHONPATH=$REPO_PATH
export PATH="/root/.local/bin:$PATH"
# We need to set NCCL_CUMEM_ENABLE=0 for performance reasons; see:
# https://github.com/vllm-project/vllm/issues/5723#issuecomment-2554389656
export NCCL_CUMEM_ENABLE=0


echo CURRENT_DATETIME=$CURRENT_DATETIME
echo PYTHONPATH=$PYTHONPATH
echo PATH=$PATH

# python3 -c "import os, ray; print(os.path.dirname(ray.__file__))"

# Without a hostname, `getent hosts` lists every host instead of failing, so skip the lookup.
BEAKER_LEADER_REPLICA_IP=""
if [ -n "$BEAKER_LEADER_REPLICA_HOSTNAME" ]; then
    BEAKER_LEADER_REPLICA_IP=$(getent hosts ${BEAKER_LEADER_REPLICA_HOSTNAME} | awk '{print $1}')
fi

RAY_NODE_PORT=8888
mkdir -p "$HOME/.triton/autotune"  # Create Triton autotune cache directory to silence warnings
ray stop --force

if [ "$BEAKER_REPLICA_RANK" == "0" ]; then
    echo "Starting Ray head node"
    ray start --head --port=$RAY_NODE_PORT --dashboard-host=0.0.0.0
else
    echo "Starting Ray worker node $BEAKER_REPLICA_RANK"
    export RAY_ADDRESS="${BEAKER_LEADER_REPLICA_IP}:${RAY_NODE_PORT}"

    # Exit code reported by cleanup. It stays 0 only for a head that was reachable and then went
    # away (the normal end of a multi-node run); every setup failure sets it to 1.
    WORKER_EXIT_CODE=0

    cleanup() {
        echo "[ray_node_setup] Cleanup: stopping Ray worker and exiting ${WORKER_EXIT_CODE}"
        ray stop --force >/dev/null 2>&1 || true
        trap - TERM INT HUP EXIT
        exit "${WORKER_EXIT_CODE}"
    }

    trap cleanup TERM INT HUP EXIT

    if [ -z "$BEAKER_LEADER_REPLICA_IP" ]; then
        echo "[ray_node_setup] Cannot resolve the Ray head from BEAKER_LEADER_REPLICA_HOSTNAME='${BEAKER_LEADER_REPLICA_HOSTNAME}' (BEAKER_REPLICA_RANK='${BEAKER_REPLICA_RANK}'). Is 'replicas' missing from the task spec? Exiting 1."
        WORKER_EXIT_CODE=1
        cleanup
    fi

    # Start worker without --block so we can control lifecycle and exit code.
    if ! ray start --address="${RAY_ADDRESS}" --dashboard-host=0.0.0.0; then
        echo "[ray_node_setup] 'ray start --address=${RAY_ADDRESS}' failed. Exiting 1."
        WORKER_EXIT_CODE=1
        cleanup
    fi

    echo "[ray_node_setup] Monitoring Ray head at ${RAY_ADDRESS}"
    # Poll head availability. Exit 0 once a head that was reachable is gone, exit 1 if it never was.
    HEAD_REACHED=0
    while true; do
        if ray status --address="${RAY_ADDRESS}" >/dev/null 2>&1; then
            HEAD_REACHED=1
        else
            if [ "$HEAD_REACHED" == "1" ]; then
                echo "[ray_node_setup] Head is unreachable. Stopping worker and exiting 0."
            else
                echo "[ray_node_setup] Head at ${RAY_ADDRESS} was never reachable. Stopping worker and exiting 1."
                WORKER_EXIT_CODE=1
            fi
            cleanup
        fi
        sleep 5
    done
fi
