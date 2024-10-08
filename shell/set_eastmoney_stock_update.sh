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
logfile=~/eastmoney/runlog/"$timeday"_set_eastmoney_stock_update.sh.log

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


log "*********************** begin ********************************"

#check holiday
log "source ~/linux_server_conf/set_stock_workday.sh"
source ~/linux_server_conf/set_stock_workday.sh

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
            '#get_realdata_from_xq.py'
            'get_dragon_tiger.py'
            'test_generate_html_dragon.py'
            'test_generate_dragon_index_html.py'
            'get_season_fund.py'
            'main_holder.py'
            'get_jigou_data.py'
            'test_get_fund_analysis.py'
            '###main_day.py'
            '###test_cross_conditions_14_30.py'
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
        "$target" = "get_realdata_from_xq.py" -o\
        "$target" = "main_zlpm.py" ];then

        hh=`date '+%H'`
        judge_time=16
        judge_time2=9
        weekday=`date '+%w'`

        if [[ $hh -ge $judge_time ]] && [[ $hh -lt $judge_time2 ]] && [[ weekday -ne 0 ||  weekday -ne 6  ]];  # >=16:00
        then
            log "$hh behind $judge_time,  continue "  # shock is already close, continue
            continue
        else
            log "$hh in front of $judge_time "
        fi

        log "cd $work_path"  
        cd $work_path

    # dragon data should be later than 17:00
    elif [ "$target" = "get_dragon_tiger.py" -o \
         "$target" = "test_generate_html_dragon.py" ];then
        hh=`date '+%H'`
        judge_time=17
        if [ $hh -ge $judge_time ];  # >= 17
        then
                log "$hh behind $judge_time "
        else
                log "$hh in front of $judge_time,  continue  "
                continue
        fi
        log "cd $work_path"  
        cd $work_path
    # dragon data should be later than 17:00
    elif [ "$target" = "test_generate_dragon_index_html.py" ];then
        hh=`date '+%H'`
        judge_time=17
        if [ $hh -ge $judge_time ]; # >= 17
        then
                log "$hh behind $judge_time "
        else
                log "$hh in front of $judge_time, continue "
                continue
        fi
        log "cd $work_path"  
        cd $work_path/html

    #get holder data at 01:00 or 02:00 am
    elif [ "$target" = "main_holder.py"  ] ; then 
        hh=`date '+%H'`
        judge_time=1
        judge_time2=2
        if [[ $hh -eq $judge_time || $hh -eq $judge_time2 ]]  ;
        then
                log "$hh is equal $judge_time "
        else
                log "$hh is NOT equal $judge_time,  continue  "
                continue
        fi
        log "cd $work_path"  
        cd $work_path

    #season finance and jigou data
    elif [ "$target" = "get_season_fund.py" -o \
        "$target" = "get_jigou_data.py" ] ; then 
        hh=`date '+%H'`
        weekday=`date '+%w'`
        judge_time=0
        judge_day=6
        if [[ $hh -eq $judge_time ]] && [[$weekday -eq $judge_day ]];
        then
                log "$hh is equal $judge_time "
                log "$weekday is equal $judge_day "
        else
                log "$hh is NOT equal $judge_time "
                log "$weekday is NOT equal $judge_day ,  continue "
                continue
        fi
        log "cd $work_path"  
        cd $work_path

    elif [ "$target" = 'test_get_fund_analysis.py' ] ;then
        hh=`date '+%H'`
        judge_time=20
        if [ $hh -gt $judge_time ];
        then
                log "$hh behind $judge_time "
        else
                log "$hh in front of $judge_time ,  continue "
                continue
        fi
        log "cd $work_path/../pysnow_ball/"  
        cd $work_path/../pysnow_ball/
        log `pwd`
 
    elif [ "$target" = 'main_day.py' ] ;then
        hh=`date '+%H'`
        mm=`date '+%M'`


        judge_time=14
        judge_time2=16
        if [[ $hh -gt $judge_time ]] && [[$hh -lt $judge_time2]];  
        # 14:00
        then
                log "$hh behind $judge_time "
        else
                log "$hh in front of $judge_time ,  continue "
                continue
        fi

        judge_time=30
        if [ $mm -eq $judge_time ]; 
        # == 14:30
        then
                log "$mm behind $judge_time "
        else
                log "$mm in front of $judge_time ,  continue "
                continue
        fi


        log "cd $work_path/"  
        cd $work_path/
        log `pwd`
 
    elif [ "$target" = 'test_cross_conditions_14_30.py' ] ;then
        hh=`date '+%H'`
        mm=`date '+%M'`


        judge_time=14
        if [ $hh -gt $judge_time ];  
        # > 14:00
        then
                log "$hh behind $judge_time "
        else
                log "$hh in front of $judge_time ,  continue "
                continue
        fi

        judge_time=32
        if [ $mm -eq $judge_time ];  #== 14:32
        then
                log "$mm behind $judge_time "
        else
                log "$mm in front of $judge_time ,  continue "
                continue
        fi


        log "cd $work_path/"  
        cd $work_path/
        log `pwd`
        
    else
        log "cd $work_path/"  
        cd $work_path/
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



