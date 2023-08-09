FROM openjdk:11
WORKDIR /release
COPY ./release/config ./config
COPY ./release/bin ./bin
COPY ./release/adapter ./adapter
ADD ./release/*.sh ./
ENTRYPOINT sh -c 'chmod 777 ./application.sh' &&\
           sh -c 'bash ./application.sh start ./config/owner1.json ./config/tasks-KNN.json' &&\
           sh -c 'tail -f /dev/null'