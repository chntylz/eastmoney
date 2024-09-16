
#/bin/sh
#20190625,by aaron

max_retry=100
retry=0

timeday=`date "+%Y_%m_%d"`
logfile=~/eastmoney/runlog/"$timeday"_get_kline_data.sh.log
#日志
log() {
    if [ "$1" ]; then
        echo -e "[$(date)] - $1"  >> $logfile
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
if [[ $judge == 0 ]] ; then
    log "holiday, return"
    exit
else
    log "work day, continue"
fi


#start to do
log " cd ~/eastmoney/  && python3 main_day.py 0 >> $logfile 2>&1  "
cd ~/eastmoney/ \
       && python3 main_day.py 0 >> $logfile 2>&1  
ret=$?
echo $ret
#if ret is not 0, it means that command is failed
while [ $ret -ne 0 ]
do

    sleep 90
    log "error"
    log " cd ~/eastmoney/  && python3 main_day.py 0 >> $logfile 2>&1  "
    cd ~/eastmoney/ && python3 main_day.py 0 >> $logfile 2>&1  
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

hh=`date '+%H'`
judge_time=15

if [ $hh -lt $judge_time ];  # <15:00
then
    log "hour <15 is $hh"
    log "python3 test_cross_conditions.py 0 >> $logfile 2>&1 \ 
         && python3 test_generate_html.py 0 >> $logfile 2>&1 \
         && ~/eastmoney/shell/set_stock_generate_html.sh "

    python3 test_cross_conditions.py 0 >> $logfile 2>&1 \
    && python3 test_generate_html.py 0 >> $logfile 2>&1 \
    && ~/eastmoney/shell/set_stock_generate_html.sh
else
    log "hour >=15 is $hh"
    log "python3 test_cross_conditions.py 0 >> $logfile 2>&1 \
    && python3 test_generate_html.py 0 >> $logfile 2>&1 \
    && ~/eastmoney/shell/set_stock_generate_html.sh "

    python3 test_cross_conditions.py 0 >> $logfile 2>&1 \
    && python3 test_generate_html.py 0 >> $logfile 2>&1 \
    && ~/eastmoney/shell/set_stock_generate_html.sh 
fi


