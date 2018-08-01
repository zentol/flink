#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

source "$(dirname "$0")"/common.sh

###################################
# Wait a specific number of successful checkpoints
# to have happened
#
# Globals:
#   None
# Arguments:
#   $1: the job id
#   $2: the number of expected successful checkpoints
#   $3: timeout in seconds
# Returns:
#   None
###################################
function wait_for_number_of_checkpoints {
    local job_id=$1
    local expected_num_checkpoints=$2
    local timeout=$3
    local count=0

    echo "Starting to wait for completion of ${expected_num_checkpoints} checkpoints"
    while (($(get_completed_number_of_checkpoints ${job_id}) < ${expected_num_checkpoints})); do

        if [[ ${count} -gt ${timeout} ]]; then
            echo "A timeout occurred waiting for successful checkpoints"
            exit 1
        else
            ((count+=2))
        fi

        local current_num_checkpoints=$(get_completed_number_of_checkpoints ${job_id})
        echo "${current_num_checkpoints}/${expected_num_checkpoints} completed checkpoints"
        sleep 2
    done
}

function get_completed_number_of_checkpoints {
    local job_id=$1
    local json_res=$(curl -s http://localhost:8081/jobs/${job_id}/checkpoints)

    echo ${json_res}    | # {"counts":{"restored":0,"total":25,"in_progress":1,"completed":24,"failed":0} ...
        cut -d ":" -f 6 | # 24,"failed"
        sed 's/,.*//'     # 24
}

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-bucketing-sink-test/target/StreamingFileSinkTestProgram.jar

backup_config
set_conf_ssl
start_cluster
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start


JOB_ID=$($FLINK_DIR/bin/flink run -d -p 4 $TEST_PROGRAM_JAR -outputPath $TEST_DATA_DIR/out/result \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running ${JOB_ID}

wait_num_checkpoints "${JOB_ID}" 5

echo "Killing TM"

# kill task manager
kill_random_taskmanager

echo "Starting TM"

# start task manager again
$FLINK_DIR/bin/taskmanager.sh start

echo "Killing 2 TMs"

# kill two task managers again shortly after
kill_random_taskmanager
kill_random_taskmanager

echo "Starting 2 TMs and waiting for successful completion"

# start task manager again and let job finish
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start

EMITTED_RECORDS=0
while ((EMITTED_RECORDS < 60000)); do
    sleep 5
    EMITTED_RECORDS=$(get_operator_metric "StreamingFileSinkProgram" "StreamingFileSinkTest-Source.0" "StreamingFileSinkTestNumRecordsEmittedBySource")
done

CURRENT_NUM_CHECKPOINTS="$(get_completed_number_of_checkpoints ${JOB_ID})"
# wait for some more checkpoint to have happened
EXPECTED_NUM_CHECKPOINTS=$((CURRENT_NUM_CHECKPOINTS + 5))

wait_for_number_of_checkpoints ${JOB_ID} ${EXPECTED_NUM_CHECKPOINTS} 60

cancel_job "${JOB_ID}"

wait_job_terminal_state "${JOB_ID}" "CANCELED"

# get all lines in pending or part files
find ${TEST_DATA_DIR}/out -type f \( -iname "part-*" \) -exec cat {} + > ${TEST_DATA_DIR}/complete_result

check_result_hash "File Streaming Sink" $TEST_DATA_DIR/complete_result "01aba5ff77a0ef5e5cf6a727c248bdc3"
