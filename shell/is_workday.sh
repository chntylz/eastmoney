#/bin/sh
#20190625,by aaron

progrom_is_running(){
    echo "call progrom_is_running()"
    echo "the 0th parameter: $0"
    echo "the 1th parameter: $1"
    echo "the 2th parameter: $2"
    echo "the 3th parameter: $3"

    echo "$1 $2 $3"

    log "call progrom_is_running()"
    log "the 0th parameter: $0"
    log "the 1th parameter: $1"
    log "the 2th parameter: $2"
    log "the 3th parameter: $3"

    log "$1 $2 $3"


    # 查找所有匹配的PID，排除当前脚本的PID
    existing_pids=`pgrep -f "$1 $2 $3"`
    echo $existing_pids
    
    # 使用 if 语句和 [ -n ] 来检查字符串是否非空
    if [ -n "$existing_pids" ]; then
        echo "程序已在运行，即将退出"
        log $existing_pids
        return 1
    else
        echo "normal run "
        log "pgrep null"
        return 0
    fi
}

is_work_day(){
    #check whether date is valid, or not
    arr=('20190913','20190914', '20191002','20191003','20191004', 
    '20191005', '20191007', '20200101', '20200124', '20200127', 
    '20200128', '20200129', '20200130', '20200131', '20200406', 
    '20200501', '20200504', '20200505', '20200625', '20200626', 
    '20201001', '20201002', '20201001', '20201005', '20201006',
    '20201007', '20201008', 
    '20210101', '20210211', '20210212', '20210215', '20210216', 
    '20210217', '20210405', '20210503', '20210504', '20210505',
    '20210614', '20210920', '20210921', '20211001', '20211004',
    '20211005', '20211006', '20211007',
    '20240916', '20240917',
    '20241001', '20241002', '20241003', '20241004', '20241007',
    '20250101', '20250128', '20250129', '20250130', '20250131',
    '20250203', '20250204',

    )
    echo "call is_work_day()"
    log "call is_work_day()"
    local cur_day=$1
    #check valid day
    if [[ ("${arr[*]}" == *"$cur_day"*) ]]; then
        #holiday
        echo '0'
        return $?
    else
        #work day
        echo '1'
        return $?
    fi
}

