# Source this before running anything: source ./env.sh
#
# Three things, all of which bite on macOS if left unset:
#   JAVA_HOME       PySpark 3.5 needs Java 17+; this Mac's system Java is 16 (x86_64)
#   SPARK_LOCAL_IP  without it Spark resolves the machine hostname and dies with
#                   UnresolvedAddressException on a laptop that isn't in /etc/hosts
#   PATH            OrbStack's docker shims live in ~/.orbstack/bin and must precede
#                   /usr/local/bin, which may hold dangling Docker Desktop symlinks

export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export SPARK_LOCAL_IP="127.0.0.1"
export PATH="$HOME/.orbstack/bin:$JAVA_HOME/bin:$PATH"
