#!/bin/bash

# 添加每7天执行一次的 crontab 任务
(crontab -l 2>/dev/null; echo "0 0 */7 * * /home/ubuntu/rds_stop_script.sh >> /home/ubuntu/rds_stop.log 2>&1") | crontab -

echo "已设置 crontab 任务，每7天自动停止 RDS 实例"
echo "您可以通过以下命令查看 crontab 任务："
echo "crontab -l"
