실행방법

- 해당 폴더에서 명령어 실행 
mvn clean install

- 스톰 폴더로 이동(lib, extlib 폴더는 라이브러리 자동으로 불러와서 해당 폴더에 넣으면안됨)
- 스톰 폴더에서 해당파일 실행
명령어 : bin/storm jar run-topology-file.jar main.package.path arg1 arg2 arg3

- 해당파일 실행
bin/storm jar simple-topology-0.0.9-SNAPSHOT-jar-with-dependencies.jar com.imi.Topology 210.96.177.68 localhost simple-kafka-4 incoming console-consumer-75722

- 인자값 설명
첫번째 : 주키퍼서버
두번째 : nimbus서버 (현재 localhost)
세번째 : 생성할 토폴로지명
네번째 : 토픽명
다섯번째 : 주키퍼 노드의 /consumers 밑에 있는 consumer명