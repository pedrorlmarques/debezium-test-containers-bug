# debezium-test-containers-bug


This repo was created with the purpose of showing the error of the debezium testcontainers.

- Caused by: org.testcontainers.containers.ContainerLaunchException: Timed out waiting for log output matching '.*Session key updated.*'
- If we run the two tests in parallel sometimes the one pass passes and the other fails and the expected result should be that all tests pass.


- Run ./mvnw test  
