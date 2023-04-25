rm testlogs/*.log
for KILLPID in `ps ax | grep 'ray' | awk ' { print $1;}'`; do 
  kill -9 $KILLPID;
done
