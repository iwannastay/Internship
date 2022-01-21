#!/bin/bash
instance_dir=$GAUSS_ROOT/data/$INSTANCE
time_out=0
mode=single_node
echo $instance_dir
while [ $time_out -le 12 -a ! -d $instance_dir ]
do
        echo "waiting for init_db"
        let time_out+=1
        sleep 5
done
sleep 30
nohup $GAUSS_ROOT/app/bin/gs_ctl start -D $instance_dir -Z $mode  &
let time_out=0
while  [ $time_out -le 10 ]
do
        process=`ps aux | grep gaussdb | grep -v grep`
        if [ "$process" != "" ]
        then
                echo "instance start";
                break
        fi
        echo "instance $INSTANCE is starting"
        sleep 3
done
while true
do
        process=`ps aux | grep gaussdb | grep -v grep`
        if [ "$process" == "" ]
        then
                echo "instance down";
                exit 1
        fi
        echo "instance $INSTANCE is running"
        sleep 3
done
exit 0
