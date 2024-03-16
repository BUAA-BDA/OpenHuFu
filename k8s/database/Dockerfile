# 使用PostGIS作为基础镜像
FROM postgis/postgis:latest

# 安装pgvector需要的依赖，例如git、build-essential等（根据需要添加）
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    postgresql-server-dev-13 \
    clang

# 克隆pgvector的源代码，编译并安装
# 注意：确保下面的PostgreSQL版本与你的PostGIS镜像中的版本匹配
RUN cd /tmp \
    && git clone https://github.com/pgvector/pgvector.git \
    && cd pgvector \
    && make \
    && make install

EXPOSE 5432