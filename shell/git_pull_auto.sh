#!/usr/bin/expect  
spawn git pull  

sleep 5
expect "*Username*:"  
send "chntylz\r" 
sleep 5
expect "*Password*:"  
send "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\r"
 
expect eof  
exit 
