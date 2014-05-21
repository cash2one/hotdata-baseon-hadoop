#include "process_day_data.h"
#include "reducer.h"
#include <iostream>
#include <vector>

int print_map_uid_data(map<uid_query_t,vector<uid_one_time_info_t> > & uid_query_infos)
{
    map<uid_query_t, vector<uid_one_time_info_t> >::iterator map_iter;
    vector<uid_one_time_info_t>::iterator vec_iter;
    for(map_iter = uid_query_infos.begin(); map_iter != uid_query_infos.end(); ++map_iter) {
            cout<<"uid:"<<map_iter->first.uid<<endl;
            cout<<"query:"<<map_iter->first.query<<endl;
            for(vec_iter = map_iter->second.begin(); vec_iter != map_iter->second.end(); ++vec_iter) {
                        cout<<"cid:"<<vec_iter->cid<<",weight:"<<vec_iter->click_weight.weight<<"|"<<vec_iter->click_weight.weighted_pos_clk;
                        cout<<"|"<<vec_iter->click_weight.examine_p<<"|"<<vec_iter->click_weight.click_num;
                        cout<<",dis:"<<vec_iter->distance<<",page_num:"<<vec_iter->page_num<<",pos:"<<vec_iter->position;
                        cout<<",query:"<<vec_iter->query<<endl;
            }
    }
    return 0;
}

int process_map_uid_query(map<uid_query_t,vector<uid_one_time_info_t> > & uid_query_infos){
    map<uid_query_t,vector<uid_one_time_info_t> >::iterator map_iter;
    for(map_iter = uid_query_infos.begin(); map_iter != uid_query_infos.end(); ++map_iter){
            process_uid_of_one_query(map_iter->first, map_iter->second);
    }
    return 0;
}

int process_uid_of_one_query(const uid_query_t & uid_query,vector<uid_one_time_info_t> & vec_uid_infos){
    weight_hot_t weight_hot[e_total_num];
    vector<uid_one_time_info_t>::iterator vec_iter;
    //归一化距离
    for(vec_iter = vec_uid_infos.begin(); vec_iter != vec_uid_infos.end(); ++vec_iter){
        int dis_class = get_dis_class(vec_iter->distance);
        weight_hot[dis_class].click_weight += vec_iter->click_weight;
        weight_hot[dis_class].page_num += vec_iter->page_num;
    }

    calc_uid_query_dis_hot(weight_hot, e_total_num);
                        
    output_hot_values(uid_query,weight_hot,e_total_num);
    return 0;
}

int calc_uid_query_dis_hot(weight_hot_t *weight_hot,int size){
    for(int idx = 0; idx < size; ++idx) {
        float temp_weight = weight_hot[idx].click_weight.weighted_pos_clk;
        float examine_p = weight_hot[idx].click_weight.examine_p;
        if(temp_weight > 0 && 0 != weight_hot[idx].click_weight.click_num) {
            weight_hot[idx].hot = (temp_weight + 0.1) /sqrt((examine_p + 1));
            weight_hot[idx].hot *= log_2(1 + log_2(2 + weight_hot[idx].page_num/weight_hot[idx].click_weight.click_num));
        }
        else {
            weight_hot[idx].hot = 0;
        }
        if(examine_p > 0 && 0 == weight_hot[idx].hot) {
            weight_hot[idx].hot = 0.01;
        }
    }
    return 0;
}

int output_hot_values(const uid_query_t & uid_query,weight_hot_t * weight_hot,int size){
    for(int idx = 0; idx < size; ++idx) {
        if(weight_hot[idx].hot > 0) {
            //输出到文件
            std::cout<<uid_query.uid<<"||"<<uid_query.query.c_str()<<"||"<<g_dis_value[idx]<<"||"<<weight_hot[idx].hot<<"||";
            std::cout<<weight_hot[idx].page_num<<"||"<<weight_hot[idx].click_weight.weight<<"||"<<weight_hot[idx].click_weight.weighted_pos_clk<<"||";
            std::cout<<weight_hot[idx].click_weight.examine_p<<"||"<<weight_hot[idx].click_weight.click_num<<std::endl;
            // ++m_uid_query_dis_num;
        }
    }
    return 0;
}

int main(int argc,char **argv,char **env)
{
    //int error_line_num = 0;
    //int ret = 0;

    std::string last_session;
    //把输出的uid信息保存起来
    uid_one_time_info_t temp_uid_one_time_info;
    uid_query_t temp_uid_query;
    uint64_t last_uid;
    int index = 0;

    map<uid_query_t,vector<uid_one_time_info_t> > uid_query_infos;
    map<uid_query_t,vector<uid_one_time_info_t> >::iterator map_iter;
    
    bool isprocess = false;
    for(std::string line;std::getline(std::cin,line);++index){
        vector<std::string> vec_buf; 
        SplitItems(line,"\t",vec_buf);

        last_uid = strtoul(vec_buf[0].c_str(),NULL,10);
        temp_uid_query.uid = last_uid;
        temp_uid_query.query = vec_buf[1];
        temp_uid_one_time_info.cid = atoi(vec_buf[2].c_str()); 
        temp_uid_one_time_info.click_weight.weight = atof(vec_buf[3].c_str());
        temp_uid_one_time_info.click_weight.examine_p = atof(vec_buf[4].c_str());
        temp_uid_one_time_info.click_weight.weighted_pos_clk = atof(vec_buf[5].c_str());
        temp_uid_one_time_info.click_weight.click_num = atoi(vec_buf[6].c_str());
        temp_uid_one_time_info.page_num = atoi(vec_buf[7].c_str());
        temp_uid_one_time_info.position = strtoul(vec_buf[8].c_str(),NULL,10);
        temp_uid_one_time_info.distance = strtoul(vec_buf[9].c_str(),NULL,10);

        map_iter = uid_query_infos.find(temp_uid_query); 
        if(map_iter == uid_query_infos.end()){
            if(index != 0){
                process_map_uid_query(uid_query_infos);
                uid_query_infos.clear();
            }
            vector<uid_one_time_info_t> temp_uid_one_time_infos; 
            temp_uid_one_time_infos.push_back(temp_uid_one_time_info);
            uid_query_infos[temp_uid_query] = temp_uid_one_time_infos;
        }else{
            map_iter->second.push_back(temp_uid_one_time_info);
        }
        //std::cout<<"-------------------------------------------\t"<<uid_query_infos.size()<<std::endl;;
        //print_map_uid_data(uid_query_infos);
        //std::cout<<"**************************************************\n";

        //process_map_uid_query(uid_query_infos);
        //uid_query_infos[temp_uid_query] = temp_uid_one_time_info;
        //uid_query_infos.insert(pair<uid_query_t &,uid_one_time_info_t &>(temp_uid_query,temp_uid_one_time_info));
        //std::cout<<"uid query info:\t"<<temp_uid_query.uid<<"\t"<<temp_uid_query.query<<std::endl;

    }
    //处理结果，输出数据

}
