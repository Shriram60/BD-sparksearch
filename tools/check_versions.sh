#!/usr/bin/env bash
# Checks this machine against versions.env. Exit code 0 only if everything matches.
#   Nodes 1-3:  ./tools/check_versions.sh
#   Node 4:     ./tools/check_versions.sh node4
# Paste the FINGERPRINT line into the team chat: all of nodes 1-3 must print the same one.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/versions.env"
ROLE="${1:-cluster}"
FAIL=0

row() {  # name, want, have
  local status=OK
  [[ "$2" == "$3" ]] || { status=FAIL; FAIL=1; }
  printf '%-8s %-22s %-22s %s\n' "$status" "$1" "want $2" "have ${3:-missing}"
}

py_have="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null)"

if [[ "$ROLE" == "node4" ]]; then
  py_ok="$(python3 -c 'import sys; print(sys.version_info >= (3, 10))')"
  row python ">=3.10" "$([[ $py_ok == True ]] && echo ">=3.10" || echo "$py_have")"
  while IFS='=' read -r pkg want; do
    [[ -z "$pkg" ]] && continue
    have="$(python3 -m pip show "$pkg" 2>/dev/null | awk '/^Version:/{print $2}')"
    row "$pkg" "$want" "$have"
  done < <(sed 's/==/=/' "$HERE/requirements-node4.txt")
  echo "FINGERPRINT node4 python=$py_have $(sed 's/==/=/' "$HERE/requirements-node4.txt" | tr '\n' ' ')"
  exit $FAIL
fi

java_have="$(java -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+).*/\1/')"
hadoop_have="$("$HADOOP_HOME/bin/hadoop" version 2>/dev/null | head -1 | awk '{print $2}')"
spark_have="$(grep -oE 'Spark [0-9.]+' "$SPARK_HOME/RELEASE" 2>/dev/null | awk '{print $2}')"
scala_have="$(ls "$SPARK_HOME/jars" 2>/dev/null | grep -oE '^scala-library-2\.[0-9]+' | sed 's/scala-library-//')"
iceberg_jars="$(ls "$SPARK_HOME/jars" 2>/dev/null | grep '^iceberg-spark-runtime' | tr '\n' ' ' | sed 's/ $//')"

row java "$JAVA_MAJOR" "$java_have"
row hadoop "$HADOOP_VERSION" "$hadoop_have"
row spark "$SPARK_VERSION" "$spark_have"
row scala "$SCALA_BINARY" "$scala_have"
row iceberg-jar "$ICEBERG_JAR" "$iceberg_jars"
row python "$PYTHON_MINOR" "$py_have"

# A pip-installed pyspark of a different version shadows the cluster's Spark (CONTEXT §4.1).
pip_pyspark="$(python3 -m pip show pyspark 2>/dev/null | awk '/^Version:/{print $2}')"
if [[ -n "$pip_pyspark" && "$pip_pyspark" != "$SPARK_VERSION" ]]; then
  row pip-pyspark "none or $SPARK_VERSION" "$pip_pyspark"
fi

echo "FINGERPRINT java=$java_have hadoop=$hadoop_have spark=$spark_have scala=$scala_have iceberg=${iceberg_jars:-none} python=$py_have"
exit $FAIL
