#!/bin/sh
#20190625,by aaron

max_retry=100
retry=0

work_path=~/eastmoney/
cd $work_path

timeday=`date "+%Y_%m_%d_%w"`
logfile=~/eastmoney/runlog/"$timeday"_get_xq_data.sh.log
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
        echo -e "[$(date)] - $1"  >> $logfile
        #sed -i "1i [$(date)] - $1 " $logfile
    fi
}



log "************ begin ********************************************************************************************"

#check holiday
log "source ~/eastmoney/shell/is_workday.sh"
source ~/eastmoney/shell/is_workday.sh

input=1
sub_str=`date -d "$input day ago" +"%Y%m%d"`
sub_str=`date +"%Y%m%d"`
log "today is $sub_str" >> $logfile
judge=$(is_work_day $sub_str)
if [[ $judge = "holiday" ]] ; then
    log "holiday, return"
    exit
else
    log "work day, continue"
fi


#start to do
log " cd ~/eastmoney/  && python3 xq_main_basic_kday.py 0 >> $logfile 2>&1  "
cd ~/eastmoney/ \
       && python3 xq_main_basic_kday.py 0 >> $logfile 2>&1  
ret=$?
echo $ret
#if ret is not 0, it means that command is failed
while [ $ret -ne 0 ]
do

    sleep 90
    log "error"
    log " cd ~/eastmoney/  && python3 xq_main_basic_kday.py 0 >> $logfile 2>&1  "
    cd ~/eastmoney/ && python3 xq_main_basic_kday.py 0 >> $logfile 2>&1  
    ret=$?
    
    if [ $ret -eq 0] 
    then 
        break
    fi

    retry=$[$retry+1]
    if [ $retry -ge $max_retry ]
    then
        break
    fi

done

exit

