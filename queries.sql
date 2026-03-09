-- 1. 创建 Hive 视图，提取 DB、Table 和 Partition 信息
-- 假设 Hive 默认路径为 /user/hive/warehouse/
CREATE OR REPLACE VIEW hive_files AS
WITH base AS (
    SELECT
        *,
        -- 移除前缀获取相对路径
        replace(path, '/user/hive/warehouse/', '') AS rel_path
    FROM inodes
    WHERE type = 'FILE' AND path LIKE '/user/hive/warehouse/%'
),
parsed AS (
    SELECT
        *,
        -- 提取 DB: 如果第一级目录带 .db 结尾则是 DB 名，否则归为 default
        CASE 
            WHEN rel_path LIKE '%.db/%' THEN split_part(rel_path, '.db/', 1)
            ELSE 'default'
        END AS db,
        -- 提取 Table: 如果有 .db，则第二级是 Table；否则第一级是 Table
        CASE 
            WHEN rel_path LIKE '%.db/%' THEN split_part(split_part(rel_path, '.db/', 2), '/', 1)
            ELSE split_part(rel_path, '/', 1)
        END AS tbl
    FROM base
)
SELECT 
    *,
    -- 提取 Partition: 路径中位于 Table 之后、文件名之前的部分
    -- 这里通过移除 DB/Table 前缀和文件名来估算
    regexp_replace(
        regexp_replace(rel_path, '^.*?(' || tbl || '/)', ''), 
        '/[^/]+$', ''
    ) AS partition_path
FROM parsed;

-- 2. 按 DB 统计
SELECT 
    db,
    count(*) AS file_count,
    sum(file_size) / 1024 / 1024 / 1024 AS total_size_gb,
    avg(file_size) / 1024 / 1024 AS avg_file_size_mb
FROM hive_files
GROUP BY 1
ORDER BY total_size_gb DESC;

-- 3. 按 Table 统计并找出碎片化严重的表
-- 碎片化定义：小文件多。这里按平均文件大小 < 32MB 且文件数 > 100 过滤
-- 计算碎片浪费：(count * preferred_block_size - sum(file_size))，即如果合并成满块能省多少空间
SELECT 
    db,
    tbl,
    count(*) AS file_count,
    sum(file_size) / 1024.0 / 1024.0 / 1024.0 AS total_size_gb,
    avg(file_size) / 1024.0 / 1024.0 AS avg_file_size_mb,
    -- 统计 HDFS Block 碎片 (小文件导致的块不饱和度)
    sum(CASE 
        WHEN file_size < preferred_block_size THEN (preferred_block_size - file_size) 
        ELSE (preferred_block_size - (file_size % CASE WHEN preferred_block_size = 0 THEN 128*1024*1024 ELSE preferred_block_size END)) % CASE WHEN preferred_block_size = 0 THEN 128*1024*1024 ELSE preferred_block_size END
    END) / 1024.0 / 1024.0 / 1024.0 AS estimated_block_waste_gb
FROM hive_files
GROUP BY 1, 2
HAVING avg_file_size_mb < 32 AND file_count > 100
ORDER BY estimated_block_waste_gb DESC;

-- 4. 按 Partition 维度统计最细粒度的分布
SELECT 
    db,
    tbl,
    partition_path,
    count(*) AS file_count,
    sum(file_size) / 1024 / 1024 AS total_size_mb,
    avg(file_size) / 1024 / 1024 AS avg_file_size_mb
FROM hive_files
GROUP BY 1, 2, 3
ORDER BY file_count DESC
LIMIT 100;

-- 5. 非 Hive 路径的 Top 10 目录统计 (大目录发现)
SELECT 
    regexp_extract(path, '^(/[^{/]+/[^{/]+/[^{/]+)', 1) AS dir_prefix,
    count(*) AS file_count,
    sum(file_size) / 1024 / 1024 / 1024 AS size_gb
FROM inodes
WHERE type = 'FILE' AND path NOT LIKE '/user/hive/warehouse/%'
GROUP BY 1
ORDER BY size_gb DESC
LIMIT 20;

-- 6. 存储空间占用最多的 User/Group
SELECT 
    user_name,
    group_name,
    count(*) AS file_count,
    sum(file_size) / 1024 / 1024 / 1024 AS total_gb
FROM inodes
WHERE type = 'FILE'
GROUP BY 1, 2
ORDER BY total_gb DESC;
