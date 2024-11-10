# Rust 语言训练营

## clickhouse compose
services:
  clickhouse-server:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse-server
    ports:
      - "8123:8123"
      - "9000:9000"
      - "9009:9009"
    volumes:
      - ./volume/clickhouse/data:/var/lib/clickhouse
      - ./volume/clickhouse/logs:/var/log/clickhouse-server/
      - ./volume/clickhouse/config/config.xml:/etc/clickhouse-server/config.xml
    environment:
      - CLICKHOUSE_DB=mydatabase
      - CLICKHOUSE_USER=kevin
      - CLICKHOUSE_PASSWORD=password
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    networks:
      - pgsql_default
networks:
  pgsql_default:
    external: true



##pgsql compose
services:
  web:
    image: nginx:latest
    extra_hosts:
      - "host.docker.internal:host-gateway"
    ports:
      - "80:80"
      - "8080:8080"
    volumes:
      - ./volume/nginx/nginx.conf:/etc/nginx/nginx.conf


#测试容器中安装 网络工具
apt-get update && apt-get install -y curl iputils-ping net-tools
apt install telnet

从pgsql中导出数据表内容去parquet
select * from postgresql('pgsql-db-1:5432','state','user_stats','postgres','postgres') into outfile '/var/lib/clickhouse/user_stats.parquet'


Clickhouse 中直接查询parquet文件
select count(*) from file('user_stats.parquet',Parquet) where last_visited_at>='2024-05-01'
