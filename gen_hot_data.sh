#!/bin/sh
MAIL_ADDR="longlijian@baidu.com wangcong06@baidu.com"

NEW_FILE_TAG=../get_raw_data/bin/has_new_file_to_process.tag
RAW_UTF8_DATA_PATH=../get_raw_data/bin/data
SH_LOG=../log/gen_hot_data.log
UTF8_DATA_PATH=../data/utf8_raw_data
GBK_RAW_DATA_PATH=../data/gbk_raw_data
today=`date +%Y%m%d`
RAW_HOT_DATA_PATH=../data/hot_data
ALL_HOT_FILE=$RAW_HOT_DATA_PATH/hot_all
FINAL_POI_HOT=../data/final_hot_data
OUT_PUT=../data/out_put/

echo $today start to process hot dat >> $SH_LOG
if [ ! -f $NEW_FILE_TAG ]
then
	echo "no file need to process" >> $SH_LOG
	echo "no file need to process" | mail -s "gen hot data failed" $MAIL_ADDR;
	exit 1
fi
rm -f $NEW_FILE_TAG
raw_files=`ls $RAW_UTF8_DATA_PATH/logfile_*`
if [ $? -ne 0 ]
then
	echo "there is no file in raw_utf8_data" >> $SH_LOG
	echo "there is no file in raw_utf8_data" | mail -s "gen hot data failed" $MAIL_ADDR;
	exit 0
fi
for file in $raw_files
do
	mv $file $UTF8_DATA_PATH
done

cd $UTF8_DATA_PATH
utf8_files=`ls logfile_*`
for file in $utf8_files
do
	iconv -f utf8 -t gbk $file > gbk_$file -c
	rm $file
	mv gbk_$file ../gbk_raw_data/
done
cd -


#gbk_files=`ls $GBK_RAW_DATA_PATH/gbk_*`
#for file in $gbk_files
#do
#	echo "process_day_data,start to process $file" >>$SH_LOG;
##now all files need to be processed are in $GBK_RAW_DATA_PATH
##the following function will do those things:
##each day's session_data will be splited and stored at $GBK_RAW_DATA_PATH and all subfiles in the
##director which have the same name with the file
##**********eachday_data default splited into 4 pieces,and you can change the file
##"./split_eachday_data.py by you purpose***** 
#    ./split_eachday_data.py $file
#    if [ $? -eq 0 ]
#    then
#        echo "split file success"
#        echo "have sucessed split one day file " >> $SH_LOG
#    else 
#        echo "split file failed"
#        echo "split one day file failed" >> $SH_LOG
#    fi
##the following line is the old version,and the line after that is a new process way
##	./process_day_data $file
##you didn't imput anython the ./process_day_data.py will automatically processing all the thing
#    ./process_day_data.py
#   	if [ $? -eq 0 ]
#   	then
#        echo "./process_day_data success"
#   	else
#    	echo "process_day_data error" | mail -s "gen hot data failed" $MAIL_ADDR;
#    	exit -1
#   	fi
#done

#'''
##将多天的热度数据汇成一个文件
#hot_files=`ls -t $RAW_HOT_DATA_PATH/hot_dat.*`
#process_hot_num=0
#if [ -f $ALL_HOT_FILE ]
#then
#	rm -f $ALL_HOT_FILE
#fi
#for file in $hot_files
#do
#	let process_hot_num+=1
#	if [ $process_hot_num -gt 60 ]
#	then
#		break
#	fi
#	cat $file >> $ALL_HOT_FILE
#done
#
##输出热度数据
#if [ -f $ALL_HOT_FILE ]
#then
#	echo "start gen_final_hot_data" >>$SH_LOG;
#	./gen_final_hot_data $ALL_HOT_FILE
#	if [ $? -eq 0 ]
#	then
#		cp $FINAL_POI_HOT/poi_* $OUT_PUT
#	else
#		echo "gen_final_hot_data error" | mail -s "gen hot data failed" $MAIL_AD;
#	fi
#else
#	echo "gen hot data error" | mail -s "gen hot data failed" $MAIL_ADDR;
#	exit 1
#fi
#'''
echo "all done" >>$SH_LOG;

exit 0 
