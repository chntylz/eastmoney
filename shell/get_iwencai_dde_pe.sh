#!/bin/sh
#20251001,by aaron

# 设置工作路径和日志文件
work_path=~/eastmoney/
cd $work_path
mkdir -p runlog

timeday=`date "+%Y_%m_%d_%w"`
logfile=~/eastmoney/runlog/"$timeday"_get_iwencai_dde_pe.sh.log

# 日志函数
log() {
    if [ "$1" ]; then
        echo -e "[$(date)] - $1"  >> $logfile
    fi
}

# 检查日志文件是否存在，如果不存在则创建并写入启动信息
if [ ! -f $logfile ]; then
    echo -e "[$(date)] - start..."  >> $logfile
fi

log "************ begin ********************************************************************************************"

# 检查是否为假日，如果是假日则直接退出
log "source ~/eastmoney/shell/is_workday.sh"
source ~/eastmoney/shell/is_workday.sh

sub_str=`date +"%Y%m%d"`
log "today is $sub_str"
judge=$(is_work_day $sub_str)

if [[ $judge = "holiday" ]] ; then
    log "holiday, return"
    exit
else
    log "work day, continue"
fi

# 获取当前星期几（0表示周日，1表示周一，以此类推）
weekday=`date '+%w'`
log "current weekday is $weekday"

# 根据星期几决定运行哪个Python脚本
if [ $weekday -eq 1 ] || [ $weekday -eq 2 ] || [ $weekday -eq 3 ] || [ $weekday -eq 4 ] || [ $weekday -eq 5 ]; then
    # 星期一到星期五运行 get_iwencai_selenium_dde.py
    log "Monday to Friday, running get_iwencai_selenium_dde.py"
    cd $work_path && python3 get_iwencai_selenium_dde.py >> $logfile 2>&1
elif [ $weekday -eq 0 ] || [ $weekday -eq 6 ]; then
    # 星期六和星期日运行 get_iwencai_selenium_pe.py
    log "Saturday or Sunday, running get_iwencai_selenium_pe.py"
    cd $work_path && python3 get_iwencai_selenium_pe.py >> $logfile 2>&1
fi

log "************ end ********************************************************************************************"