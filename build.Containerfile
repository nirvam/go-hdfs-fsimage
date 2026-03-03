# 使用 RHEL 8 基础镜像确保 glibc 兼容性 (glibc 2.28)
FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

# 安装构建必需的 C++ 工具链 (DuckDB CGO 需要)
RUN microdnf install -y 
    gcc 
    gcc-c++ 
    make 
    tar 
    gzip 
    && microdnf clean all

# 安装指定版本的 Go (1.25.0)
# 针对 RHEL 8 官方源通常版本较低，我们直接从官网下载二进制
ENV GO_VERSION=1.25.0
RUN curl -L https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz | tar -C /usr/local -xzf -
ENV PATH=$PATH:/usr/local/go/bin

WORKDIR /src

# 预下载依赖以利用缓存
COPY go.mod go.sum ./
RUN go mod download

# 默认构建指令
CMD ["go", "build", "-ldflags", "-s -w", "-o", "hdfs-vdisk-rhel8", "./cmd/hdfs-vdisk"]
