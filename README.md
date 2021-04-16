# debezium-test-containers-bug


This repo was created with the purpose of showing the failure to capture the outbox event using a PostgresSQL connector.
In addition, it's also possible to observe the initialization error of the Debezium Testcontainers shown below:
```
Caused by: org.testcontainers.containers.ContainerLaunchException: Timed out waiting for log output matching '.*Session key updated.*'
```
If we run the two tests together, sometimes one passes and the other fails and the expected result should be that all tests pass.