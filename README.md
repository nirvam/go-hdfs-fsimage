# Go-HDFS-FSImage

高性能 HDFS FSImage 离线解析工具，专为处理亿级 INode 元数据分析设计。

## 核心特性
- **两遍扫描 (Two-Pass) 算法**：仅在内存中缓存紧凑的目录树骨架，文件详情采用 `Delimited Protobuf` 流式解析，内存开销极低。
- **高性能优化**：
    - **对象复用**：深度集成 `sync.Pool` 管理字节切片、Protobuf 消息和导出记录，在高并发/海量数据场景下显著降低 GC 压力。
    - **路径拼接优化**：基于 `strings.Builder` 和 `sync.Pool` 缓存路径片段，实现绝对路径生成的“零冗余分配”。
    - **高效 I/O**：重构 `ReadDelimited` 逻辑，支持 `bufio.Reader.Peek` 预读，大幅减少系统调用。
- **DuckDB 高吞吐集成**：利用 DuckDB 官方 Appender 接口绕过 SQL 层，直接将元数据写入内存缓冲区，吞吐量远超传统 SQL `INSERT`。
- **RHEL 8 生产环境兼容**：内置 Podman 构建流程，确保在 Arch Linux 等现代系统上也能构建出适配 RHEL 8 (glibc 2.28) 的兼容性二进制文件。

## 快速开始

### 1. 生成代码与本地构建
```bash
make proto  # 生成 Protobuf Go 代码 (需 protoc 和 protoc-gen-go)
make build  # 本地编译二进制文件 (./cmd/hdfs-vdisk)
```

### 2. 跨平台兼容构建 (RHEL 8 / CentOS 8)
如果你在现代 Linux (如 Arch) 上开发，但需要在旧版生产环境运行，请执行：
```bash
make build-rhel8  # 利用 Podman/UBI8 容器化构建
```

### 3. 执行解析
```bash
# 导出为 DuckDB (推荐，用于复杂 SQL 分析)
./hdfs-vdisk-rhel8 -i fsimage_0000000000000012345 -t duckdb -o metadata.duckdb

# 导出为标准 CSV
./hdfs-vdisk-rhel8 -i fsimage_0000000000000012345 -t csv -o report.csv
```

## 技术路线 (Roadmap & TODO)
- [x] 基于两遍扫描的流式解析框架。
- [x] DuckDB Appender 高吞吐导出支持。
- [x] `sync.Pool` 全方位性能优化。
- [x] RHEL 8 生产环境 glibc 兼容性方案。
- [ ] **并发解析尝试**：探索多 Section 并行扫描（如果镜像支持 Section 分片读取），以进一步压榨多核性能。
- [ ] **多压缩格式支持**：支持 LZ4/Snappy 等 Hadoop 常用压缩算法（当前仅支持 Gzip）。

## 开发与贡献
项目遵循 `PROJECT.md` 中的架构规范。所有代码均需通过 `go test ./...` 验证。
针对 DuckDB 相关的修改，请务必检查连接泄露问题（已在 `pkg/exporter/duckdb.go` 中提供标准实现参考）。
