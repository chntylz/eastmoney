
#/bin/sh
#20190625,by aaron

max_retry=100
retry=0

work_path=~/eastmoney/
cd $work_path

timeday=`date "+%Y_%m_%d"`
logfile=~/eastmoney/runlog/"$timeday"_get_dragon_data.sh.log

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
log " cd ~/eastmoney/  && python3 get_dragon_tiger.py 0 >> $logfile 2>&1  "
cd ~/eastmoney/ \
       && python3 get_dragon_tiger.py 0 >> $logfile 2>&1  
ret=$?
echo $ret
#if ret is not 0, it means that command is failed
while [ $ret -ne 0 ]
do

    sleep 90
    log "error"
    log " cd ~/eastmoney/  && python3 get_dragon_tiger.py 0 >> $logfile 2>&1  "
    cd ~/eastmoney/ && python3 get_dragon_tiger.py 0 >> $logfile 2>&1  
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



#start
file_array=(
            'test_generate_html_dragon.py'
            'test_generate_dragon_index_html.py'
           )


for value in ${file_array[@]}
do
    target=$value
    log " -- for loop $target --" 

    cd $work_path

    if [ "$target" = "test_generate_html_dragon.py" ];then
        log "cd $work_path"  
        cd $work_path
    elif [ "$target" = "test_generate_dragon_index_html.py" ];then
        log "cd $work_path/html"  
        cd $work_path/html
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


