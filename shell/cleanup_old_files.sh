#!/bin/bash

# 定义需要清理的目录列表
DIRECTORIES=(
    "/home/aaron/eastmoney/csv"   # 替换为实际目录路径
    "/home/aaron/eastmoney/runlog"   # 可添加更多目录
)

# 遍历所有目录
for dir in "${DIRECTORIES[@]}"; do
    # 检查目录是否存在
    if [ ! -d "$dir" ]; then
        echo "错误: 目录 '$dir' 不存在，已跳过"
        continue
    fi

    echo "正在处理目录: $dir"
    # 删除超过30天的文件
    find "$dir" -type f -mtime +30 -print -delete
done

echo "清理完成"

