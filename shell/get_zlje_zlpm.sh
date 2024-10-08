#/bin/bash
#20190625,by aaron


#    大于 -gt (greater than)
#    小于 -lt (less than)
#    大于或等于 -ge (greater than or equal)
#    小于或等于 -le (less than or equal)
#    不相等 -ne （not equal）
#    相等 -eq （equal）

work_path=~/eastmoney/
cd $work_path
mkdir -p runlog
time=`date "+%Y_%m_%d_%H_%M_%S"`

timeday=`date "+%Y_%m_%d"`
logfile=~/eastmoney/runlog/"$timeday"_get_zlje_zlpm.sh.log

#
#sed '1i 添加的内容' file 　　 #这是在第一行前添加字符串
#sed '$i 添加的内容' file 　　 #这是在最后一行行前添加字符串
#sed '$a添加的内容' file 　　 #这是在最后一行行后添加字符串
#

#check file if exist
if [ ! -f $logfile ]; then
    echo -e "[$(date)] - $1 start..."  >> $logfile
fi


#日志
log() {
    if [ "$1" ]; then
        #echo -e "[$(date)] - $1"  >> $logfile
        sed -i "1i [$(date)] - $1 " $logfile
    fi
}

log "*********************** begin get_zlje_zlpm.py ********************************"

#check holiday
log "source ~/eastmoney/shell/is_workday.sh"
source ~/eastmoney/shell/is_workday.sh

input=1
sub_str=`date -d "$input day ago" +"%Y%m%d"`
sub_str=`date +"%Y%m%d"`
log "today is $sub_str" >> $logfile
judge=$(is_work_day $sub_str)
if [[ $judge == 0 ]] ; then
    log "holiday, return"
    exit
else
    log "work day, continue"
fi




#start

file_array=(
            'get_daily_zlje.py'
            'main_zlpm.py'
           )


#14:30-14:40 return

hh=`date '+%H'`
mm=`date '+%M'`
if [ $hh -eq 14 -a  $mm -gt 29 -a $mm -lt 40 ]  ; then
    log 'it is 14:30~14:40 exit'
    exit
fi


for value in ${file_array[@]}
do
    log " -- for loop --" 
    target=$value
    log $target 

    cd $work_path

    #9:00~15:00
    if [ "$target" = "get_daily_zlje.py" -o \
        "$target" = "main_zlpm.py" ];then

        hh=`date '+%H'`
        judge_time=16
        judge_time2=9
        weekday=`date '+%w'`
        log "weekday is $weekday, hour is $hh"

        if [[ $hh -ge $judge_time ]] && [[ $hh -lt $judge_time2 ]] && [[ weekday -ne 0 ||  weekday -ne 6  ]];  # >=16:00
        then
            log "$hh behind $judge_time,  continue "  # shock is already close, continue
            continue
        else
            log "$hh in front of $judge_time "
        fi

        log "cd $work_path"  
        cd $work_path
   else
        log "exit"  
        exit
    fi


    time=`date "+%Y_%m_%d_%H_%M_%S"`
    log "time python3 $target start $time" 
    tt_1=`date "+ %s"`
    log "begin run python3"
    time python3 $target 0 >> $logfile    2>&1 
    tt_2=`date "+ %s"`
    time=`date "+%Y_%m_%d_%H_%M_%S"`
    log "time python3 $target stop $time, cost time is $(($tt_2 - $tt_1))" 
    
    
done

log "*********************** end ********************************" 

exit


