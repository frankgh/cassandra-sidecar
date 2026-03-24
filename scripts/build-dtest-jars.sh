#!/bin/bash
#
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
#

set -xe
CANDIDATE_BRANCHES=(
  "cassandra-4.0:bd835726f2e0ed2600c1e426bd1869d6fb1252c8"
  "cassandra-4.1:95088443ff2f4ed30ac52ef1d900049ee804b040"
  "cassandra-5.0:fb7efd62190804ead7d0dd49d70bb22c7e5e2c7c"
  "cassandra-6.0:105694f625e53785e774c82e713623adef6a21a4"
  "trunk:3ace21c90d31c18fab8eb9706a2615ba2c61d80e"
)
# Support for 4.0 will be dropped once 6.0 is officially released
BRANCHES=( ${BRANCHES:-cassandra-4.0 cassandra-4.1 cassandra-5.0 cassandra-6.0 trunk} )
echo ${BRANCHES[*]}
REPO=${REPO:-"https://github.com/apache/cassandra.git"}
SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
DTEST_JAR_DIR="$(dirname "${SCRIPT_DIR}/")/dtest-jars"
DTEST_JAR_DIR=${CASSANDRA_DEP_DIR:-$DTEST_JAR_DIR}
TARBALL_DIR="$(dirname "${SCRIPT_DIR}/")/cassandra-tarballs"
BUILD_DIR="${DTEST_JAR_DIR}/build"

if [[ "x$CLEAN" != "x" ]]; then
  echo "Clean up $DTEST_JAR_DIR"
  rm -rf $DTEST_JAR_DIR
  echo "Cleanup $TARBALL_DIR"
  rm -rf $TARBALL_DIR
fi

source "$SCRIPT_DIR/functions.sh"
mkdir -p "${BUILD_DIR}"

# host key verification
mkdir -p ~/.ssh
REPO_HOST=$(get_hostname "${REPO}")
ssh-keyscan "${REPO_HOST}" >> ~/.ssh/known_hosts || true

for index in "${!CANDIDATE_BRANCHES[@]}"; do
  cd "${BUILD_DIR}"
  branchSha=(${CANDIDATE_BRANCHES[$index]//:/ })
  branch=${branchSha[0]}
  sha=${branchSha[1]}

  if ! [[ "${BRANCHES[@]}" =~ "$branch" ]]; then
    echo "branch ${branch} is not selected to build. The selected branches are ${BRANCHES[*]}"
    continue
  fi

  echo "index ${index} branch ${branch} sha ${sha}"
  # check out the correct cassandra version:
  if [ ! -d "${branch}" ] ; then
    if [ -n "${sha}" ] ; then
      mkdir -p "${branch}"
      cd "${branch}"
      git init
      git remote add upstream "${REPO}"
      git fetch --depth=1 upstream "${sha}"
      git reset --hard FETCH_HEAD
    else
      git clone --depth 1 --single-branch --branch "${branch}" "${REPO}" "${branch}"
      cd "${branch}"
    fi
  else
    cd "${branch}"
    if [ -z "${sha}" ] ; then
      git pull
    fi
  fi
  if [ -z "${sha}" ] ; then
    git checkout "${branch}"
  fi
  git clean -fd
  CASSANDRA_VERSION=$(cat build.xml | grep 'property name="base.version"' | awk -F "\"" '{print $4}')
  # Loop to prevent failure due to maven-ant-tasks not downloading a jar.
  for x in $(seq 1 3); do
        RETURN="0"
        DTEST_JAR_FILE="${DTEST_JAR_DIR}/dtest-${CASSANDRA_VERSION}.jar"
        TARBALL_GLOB_FILE="${TARBALL_DIR}/apache-cassandra-${CASSANDRA_VERSION}*-bin.tar.gz"

        if [ -f "${DTEST_JAR_DIR}/dtest-${CASSANDRA_VERSION}.jar" ] && (ls ${TARBALL_GLOB_FILE} >/dev/null 2>&1); then
          echo "Found existing dtest jar ${DTEST_JAR_FILE} and tarball `ls ${TARBALL_GLOB_FILE}` for version ${CASSANDRA_VERSION}, skipping build."
          break
        fi

        if ! [ -f "${DTEST_JAR_FILE}" ]; then
          echo "Building dtest jar ${DTEST_JAR_FILE} for version ${CASSANDRA_VERSION}"
          "${SCRIPT_DIR}/build-shaded-dtest-jar-local.sh"
          RETURN="$?"
        fi

        if [ "${RETURN}" -eq "0" ] && ! (ls ${TARBALL_GLOB_FILE} >/dev/null 2>&1); then
          echo "Building tarball for version ${CASSANDRA_VERSION}"
          "${SCRIPT_DIR}/build-cassandra-tarball.sh"
          RETURN="$?"
          break
        fi
  done
  # Exit, if we didn't build successfully
  if [ "${RETURN}" -ne "0" ]; then
      echo "Build failed with exit code: ${RETURN}"
      exit ${RETURN}
  fi
done

# Remove the build directory enclosing all Cassandra branches just cloned,
# in order to not confuse IDE's indexing
rm -rf ${BUILD_DIR}
