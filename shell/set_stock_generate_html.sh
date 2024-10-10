#/bin/sh
#20190625,by aaron

cd ~/eastmoney/html
mkdir -p runlog
time=`date "+%Y_%m_%d_%H_%M_%S"`
time=`date "+%Y_%m_%d_%w"`
logfile=~/eastmoney/runlog/"$time"_generate_html.log

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


#start
log "************ begin ********************************************************************************************"


#check holiday
log "source ~/eastmoney/shell/is_workday.sh"
source ~/eastmoney/shell/is_workday.sh


#sub_str=`date +"%Y%m%d"`
input=0
sub_str=`date -d "$input day ago" +"%Y%m%d"`
sub_str=`date +"%Y%m%d"`
log "today is $sub_str" 
judge=$(is_work_day $sub_str)
if [[ $judge == 0 ]] ; then
    log 'holiday, return'
    exit
else
    log 'work day, continue'
fi

comm_generate='test_generate_comm_index_html.py'

file_array=(
            '5days' 
            'cross3line'
            'cuptea'
            'dragon'
            'duckhead' 
            'holder'
            'jigou' 
            'macd'
            'peach' 
            'quad'
            'volume'
            'zig'
            'zlje'
            'test_generate_basic_html.py'
            'test_generate_index_html.py'
            'test_generate_hsgt_index_html.py'
            )

for value in ${file_array[@]}
do
    target=$value
    log "---------------------------- $target ---------------------------" 

    cd ~/eastmoney/html

    if [ "$target" = "test_generate_index_html.py" ];then
        comm_generate=$target
        log "cp -f $comm_generate /var/www/html/" 
        cp -f $comm_generate /var/www/html/
        log "cd /var/www/html/"  
        cd /var/www/html/
    elif [  "$target" = "5days" -o \
            "$target" = "cross3line" -o \
            "$target" = "cuptea" -o \
            "$target" = "dragon" -o \
            "$target" = "duckhead" -o \
            "$target" = "holder" -o \
            "$target" = "jigou" -o \
            "$target" = "macd" -o \
            "$target" = "peach" -o \
            "$target" = "quad" -o \
            "$target" = "volume" -o \
            "$target" = "zig" -o \
            "$target" = "zlje" ];then
        comm_generate='test_generate_comm_index_html.py'
        log "cp -f $comm_generate /var/www/html/" 
        cp -f $comm_generate /var/www/html/
        log "cd /var/www/html/"  
        cd /var/www/html/
        log "rm $target-index.html"
        rm $target-index.html
    elif [ "$target" = "test_generate_basic_html.py" ];then
        comm_generate=$target
        log "cp -f $comm_generate /var/www/html/" 
        cp -f $comm_generate /var/www/html/
        log "cd /var/www/html/"  
        cd /var/www/html/
        log "rm basic-index.html"
        rm basic-index.html
    elif [ "$target" = "test_generate_hsgt_index_html.py"  ] ; then 
        comm_generate=$target
        log "cp -f $comm_generate /var/www/html/hsgt/" 
        cp -f $comm_generate /var/www/html/hsgt/
        log "cd /var/www/html/hsgt"  
        cd /var/www/html/hsgt
    fi

    time=`date "+%Y_%m_%d_%H_%M_%S"`
    log "time python3 $comm_generate $target start $time" 
    tt_1=`date "+ %s"`
    log "#####################################################" 
    time python3 $comm_generate $target  >> $logfile 2>&1
    tt_2=`date "+ %s"`
    time=`date "+%Y_%m_%d_%H_%M_%S"`
    log "time python3 $comm_generate $target stop $time, cost time is $(($tt_2 - $tt_1))" 
    
    
done


exit


#delete the follow
#####################################################################################

