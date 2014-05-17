#! /bin/sh

#specify input output workpath
INPUT1="hdfs://nj01-nanling-hdfs.dmop.baidu.com:54310/app/vs/ns-map/longlijian/gen_hot_data/sessiondata/*"
INPUT2="hdfs://nj01-nanling-hdfs.dmop.baidu.com:54310/app/vs/ns-map/longlijian/gen_hot_data/poidata/*"
OUTPUT="hdfs://nj01-nanling-hdfs.dmop.baidu.com:54310/app/vs/ns-map/longlijian/output"

hadoop_conf_input_output(){
    hadoop fs -test -d $OUTPUT && hadoop fs -rmr $OUTPUT
}

hadoop_run(){
    hadoop streaming \
        -D mapred.job.name="baidu.llj.join" \
        -D stream.map.output.field.separator="\t" \
        -D stream.num.map.output.key.fields=2 \
        -D mapred.job.priority=VERY_HIGH \
        -D mapred.job.map.capacity=500 \
        -D mapred.job.reduce.capacity=500 \
        -D mapred.map.tasks=500 \
        -D mapred.reduce.tasks=500 \
        -D mapred.min.split.size=0x0FFFFFFFFFFFFFFF \
        -D num.key.fields.for.partition=1 \
        -D mapred.text.key.partitioner.options="-k1,1" \
        -D mapred.text.key.comparator.options="-k1,1n -k2,2n" \
        -D mapred.output.key.comparator.class="org.apache.hadoop.mapred.lib.KeyFieldBasedComparator" \
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
        -input $INPUT1 \
        -input $INPUT2 \
        -output $OUTPUT \
        -mapper "mapper.py"\
        -reducer "reduce.py" \
        -file "/home/map/longlijian/gen_hot_data/data/hadoop_join/mapper.py" \
        -file "/home/map/longlijian/gen_hot_data/data/hadoop_join/reduce.py" 
        
#    -input $INPUT2 \
#    -D map.output.key.field.separator="\t"\
#    -D mapred.text.key.comparator.options=-k1,1nr \
#    -D num.key.fields.for.partition=1 \
#    -D num.key.fields.for.partition=10 \
#    -mapper "python  /user/ns-map/hotdata/map/split_eachday_data.py $input" \
#    -cacheArchive "/share/python26.tar.gzpython26" \
#    -cacheArchive "$pkgpath$app"
#    -mapper cat \
#    -mapper "python  /user/ns-map/hotdata/map/split_eachday_data.py $input" \
#    -mapper cat \
}
    
main(){
    hadoop_conf_input_output
    hadoop_run
}
main
