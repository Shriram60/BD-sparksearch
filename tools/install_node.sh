#!/usr/bin/env bash
# Installs the pinned toolchain from versions.env so every node runs identical versions.
#   Nodes 1-3:  ./tools/install_node.sh
#   Node 4:     ./tools/install_node.sh node4
# Safe to rerun: anything already at the right version is skipped. A wrong version already in
# /opt is never deleted automatically; the script stops and tells you what to remove.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/versions.env"
ROLE="${1:-cluster}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

say() { printf '\n==> %s\n' "$*"; }
die() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }

verify_sha512() {  # file, url-of-.sha512
  local want got
  want="$(curl -fsSL "$2" | tr -d ' \n' | grep -oiE '[0-9a-f]{128}' | tr 'A-F' 'a-f')"
  got="$(sha512sum "$1" | cut -d' ' -f1)"
  [[ "$want" == "$got" ]] || die "checksum mismatch for $1"
}

if [[ "$ROLE" == "node4" ]]; then
  say "Node 4: Python packages"
  python3 -m pip install -r "$HERE/requirements-node4.txt"
  exec "$HERE/check_versions.sh" node4
fi

say "Java $JAVA_MAJOR"
if ! dpkg -s "openjdk-${JAVA_MAJOR}-jdk" >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y "openjdk-${JAVA_MAJOR}-jdk" ssh rsync curl
fi
JDK_DIR="$(ls -d /usr/lib/jvm/java-${JAVA_MAJOR}-openjdk-* | head -1)"
# Make Java 11 the default if another JDK is installed too.
sudo update-alternatives --set java "$JDK_DIR/bin/java" >/dev/null

say "Hadoop $HADOOP_VERSION"
if [[ -x "$HADOOP_HOME/bin/hadoop" ]]; then
  have="$(JAVA_HOME="$JDK_DIR" "$HADOOP_HOME/bin/hadoop" version 2>/dev/null | head -1 | awk '{print $2}')"
  [[ "$have" == "$HADOOP_VERSION" ]] || die "$HADOOP_HOME has Hadoop $have. Move it away (sudo mv $HADOOP_HOME ${HADOOP_HOME}.old) and rerun."
  echo "already $have"
else
  curl -fL -o "$TMP/hadoop.tgz" "$HADOOP_URL"
  verify_sha512 "$TMP/hadoop.tgz" "$HADOOP_URL.sha512"
  sudo tar -xzf "$TMP/hadoop.tgz" -C /opt
  sudo mv "/opt/hadoop-$HADOOP_VERSION" "$HADOOP_HOME"
  sudo chown -R "$USER:$USER" "$HADOOP_HOME"
fi

say "Spark $SPARK_VERSION"
if [[ -x "$SPARK_HOME/bin/spark-submit" ]]; then
  have="$(cat "$SPARK_HOME/RELEASE" 2>/dev/null | grep -oE 'Spark [0-9.]+' | awk '{print $2}')"
  [[ "$have" == "$SPARK_VERSION" ]] || die "$SPARK_HOME has Spark $have. Move it away (sudo mv $SPARK_HOME ${SPARK_HOME}.old) and rerun."
  echo "already $have"
else
  curl -fL -o "$TMP/spark.tgz" "$SPARK_URL"
  verify_sha512 "$TMP/spark.tgz" "$SPARK_URL.sha512"
  sudo tar -xzf "$TMP/spark.tgz" -C /opt
  sudo mv "/opt/spark-$SPARK_VERSION-bin-hadoop3" "$SPARK_HOME"
  sudo chown -R "$USER:$USER" "$SPARK_HOME"
fi

say "Iceberg $ICEBERG_VERSION"
# Any other Iceberg runtime on the classpath would clash with the pinned one.
find "$SPARK_HOME/jars" -name 'iceberg-spark-runtime-*.jar' ! -name "$ICEBERG_JAR" -print -delete
if [[ ! -f "$SPARK_HOME/jars/$ICEBERG_JAR" ]]; then
  curl -fL -o "$TMP/$ICEBERG_JAR" "$ICEBERG_URL"
  [[ "$(curl -fsSL "$ICEBERG_URL.sha1")" == "$(sha1sum "$TMP/$ICEBERG_JAR" | cut -d' ' -f1)" ]] \
    || die "checksum mismatch for $ICEBERG_JAR"
  mv "$TMP/$ICEBERG_JAR" "$SPARK_HOME/jars/"
fi

say "Environment"
PROFILE_LINE='[ -f /opt/spark/conf/cluster-env.sh ] && . /opt/spark/conf/cluster-env.sh'
grep -qxF "$PROFILE_LINE" ~/.bashrc || echo "$PROFILE_LINE" >> ~/.bashrc
cat > "$TMP/cluster-env.sh" <<EOF
export JAVA_HOME=$JDK_DIR
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$SPARK_HOME
export PATH=\$SPARK_HOME/bin:\$SPARK_HOME/sbin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH
EOF
mv "$TMP/cluster-env.sh" "$SPARK_HOME/conf/cluster-env.sh"
echo "open a new terminal (or: source ~/.bashrc) to pick up PATH"

exec "$HERE/check_versions.sh" "$ROLE"
