#!/bin/bash
instance_dir=$GAUSS_ROOT/data/$INSTANCE_ID
time_out=0



echo $instance_dir
while [ $time_out -le 12 -a ! -d $instance_dir ]
do
        echo "waiting for init_db"
        let time_out+=1
        sleep 5
done
sleep 60


if [ X$SINGLE == X"true" ]
then
	start_cmd="$GAUSS_ROOT/app/bin/gs_ctl start -D $instance_dir -Z single_mode"
elif [[ $INSTANCE_ID =~ "-0" ]]
then
	start_cmd="$GAUSS_ROOT/app/bin/gs_ctl start -D $instance_dir -M primary"
elif [[ X"`ps aux | grep gaussdb | grep -v grep`"==X"" ]]
then
	start_cmd="$GAUSS_ROOT/app/bin/gs_ctl build -D $instance_dir -b full"
else
	start_cmd="echo 'gaussdb instance exists, there is nothing to do.'"
fi
echo "$start_cmd"
nohup $start_cmd &


let time_out=0
while  [ $time_out -le 10 ]
do
        process=`ps aux | grep gaussdb | grep -v grep`
        if [ "$process" != "" ]
        then
                echo "instance start";
                break
        fi
        echo "instance $INSTANCE_ID is starting"
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
        echo "instance $INSTANCE_ID is running"
        sleep 3
done
exit 0
