1 - flink location folder MacOS install used brew : **brew install apache-flink**

    /opt/homebrew/Cellar/apache-flink/1.17.1/libexec

2 - run flink cluster

    ./bin/start-cluster.sh

3 - stop flink cluster

    ./bin/stop-cluster.sh

4 - url flink UI

    http://localhost:8081/#/overview

5 - run cli

    ./bin/flink run /Users/oleksandrstepurko/IdeaProjects/ApacheFlinkForDummies/FlinkWindow/target/FlinkWindow-1.0-SNAPSHOT.jar

6 - view logs from console

    tail -f log/flink-*-taskexecutor-*.out

7 - list of running jobs

    ./bin/flink list

