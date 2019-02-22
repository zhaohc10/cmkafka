# Modify this file to contain the pre-commit hook for kafka.
# See http://github.mtv.cloudera.com/CDH/sentry/blob/cdh5-1.5.1/cloudera/pre_commit_hook.sh for an example.
./gradlew cleanTest testWithFlakyRetry -PmaxParallelForks=1 -PscalaVersion=2.11
