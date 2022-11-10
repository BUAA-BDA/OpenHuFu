FROM apachekylin/apache-kylin-standalone:4.0.0

COPY ./entrypoint.sh /opt/hive/
COPY ./init /opt/hive/init
RUN chmod 777 /opt/hive/entrypoint.sh
RUN chmod -R 777 /opt/hive/init

SHELL ["/bin/bash", "-c"]

ENTRYPOINT ["/opt/hive/entrypoint.sh"]