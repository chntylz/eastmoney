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
logfile=~/eastmoney/runlog/"$timeday"_get_holder_jijin_fina_balance_income_cashflow.sh.log


#日志
log() {
    if [ "$1" ]; then
        echo -e "[$(date)] - $1"  >> $logfile
    fi
}

log "     "
log "*********************** begin ********************************"

#check holiday
log "source ~/linux_server_conf/set_stock_workday.sh"
source ~/linux_server_conf/set_stock_workday.sh

input=1

#start
file_array=(
            'main_holder.py'
            'get_jigou_data.py'
            'main_fina.py'
            'main_balance.py'
            'main_income.py'
            'main_cashflow.py'
            'get_sina_fina_data.py'
           )



for value in ${file_array[@]}
do
    target=$value
    log " -- for loop $target  --" 

    cd $work_path

    if [ "$target" = "main_holder.py" -o \
         "$target" = "get_jigou_data.py" -o \
         "$target" = "main_fina.py" -o \
         "$target" = "main_balance.py" -o \
         "$target" = "main_income.py" -o \
         "$target" = "main_cashflow.py" ] ; then 
        input=1
        log "cd $work_path"  
        cd $work_path

    else
        log "cd $work_path/"  
        cd $work_path/
    fi


    time=`date "+%Y_%m_%d_%H_%M_%S"`
    log "time python3 $target $input start $time" 
    tt_1=`date "+ %s"`
    log "begin run python3"
    time python3 $target $input >> $logfile    2>&1 
    tt_2=`date "+ %s"`
    time=`date "+%Y_%m_%d_%H_%M_%S"`
    log "time python3 $target $input stop $time, cost time is $(($tt_2 - $tt_1))" 
    
    
done

log "*********************** end ********************************" 

exit



