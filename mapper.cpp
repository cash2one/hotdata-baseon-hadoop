#include "process_day_data.h"
#include "mapper.h"
#include <iostream>
#include <vector>
#include <sstream>
#include <fstream>

int print_log_info(const session_key_t & session_key,vector<log_info_t> & session_info){
    vector<log_info_t>::iterator it;
    for(it = session_info.begin();it != session_info.end();++it){
        std::cout<<session_key.session_id<<"\t"<<it->step<<"\t"<<session_key.cuid<<"\t";    
        std::cout<<it->step<<"\t"<<it->action_id<<"\t"<<it->query_type<<"\t";
        std::cout<<it->page_num<<"\t"<<it->request_num<<"\t"<<it->bound<<"\t";
        std::cout<<it->city_id<<"\t"<<it->click_uid<<"\t"<<it->query<<"\t";
        std::cout<<it->vec_uids.capacity()<<"\t"<<"test"<<"\t"<<"("<<it->coordinate.x<<","<<it->coordinate.y<<")\t";
        std::cout<<"("<<it->coordinate_result.x<<","<<it->coordinate_result.y<<")\n";
        }
    return 0;
}

int get_click_class(const uint64_t action_id,const set<uint64_t> & satisfy_action_ids,const set<uint64_t> & interest_action_ids){
    if(satisfy_action_ids.end() != satisfy_action_ids.find(action_id)){
        return CLICK_SATISFY_CLASS; 
    }
    if(interest_action_ids.end() != interest_action_ids.find(action_id)){
        return CLICK_INTEREST_CLASS; 
    }
    
    return -1;
}

//把一次session分割成多次的查询行为
int dispatch_session(vector<log_info_t> & session_info,vector<search_action_pos_t> & vec_search_action,const conf_info_t & conf_info){
    if(session_info.size() <= 1){
        return 0;
    }
    vector<log_info_t>::iterator vec_iter;
    int cur_action = 0;
    int start_action = 0;
    int end_action = 0;
    bool new_search = true;
    
    int last_step = 0;
    const set<string> & set_search_type = conf_info.set_search_type; 
    for(vec_iter = session_info.begin();vec_iter != session_info.end(); ++vec_iter,++cur_action){
        if(set_search_type.end() != set_search_type.find(vec_iter->query_type)){
//            std::cout<<"just test\t"<<vec_iter->step<<std::endl; 
            //其实只是统计在第几步有查询动作
            if(new_search){
                start_action = vec_iter->step;
                last_step = vec_iter->step;
                new_search = false;
            }
            else{
                if(last_step == vec_iter->step){
                    continue;
                }
                else{
                    last_step = vec_iter->step;
                    end_action = vec_iter->step;
                    search_action_pos_t temp_action_pos;
                    temp_action_pos.start_pos = start_action;
                    temp_action_pos.end_pos = end_action;
                    vec_search_action.push_back(temp_action_pos);
                    start_action = end_action;
                }
            }
        }
    }
    if(!new_search && session_info.back().step > (int)(start_action+1)){
        search_action_pos_t temp_action_pos;
        temp_action_pos.start_pos = start_action;
        temp_action_pos.end_pos = session_info.back().step;
        vec_search_action.push_back(temp_action_pos);
    }
    return 0;
}



int process_one_session(const session_key_t & session_key,vector<log_info_t> & session_info,list<session_show_click_info_t> & session_click_info,const conf_info_t & conf_info){
    vector<search_action_pos_t> vec_search_action;
    dispatch_session(session_info,vec_search_action,conf_info);
    if(0 == vec_search_action.size()){
//        std::cout<<"vec_search_action empyt"<<std::endl;
        return 0; 
    }
    else{
        vector<search_action_pos_t>::iterator it;
//        std::cout<<"search_action start and end"<<std::endl;
        for(it = vec_search_action.begin();it != vec_search_action.end();++it)
        {
//            std::cout<<it->start_pos<<"\t"<<it->end_pos<<std::endl;
        }
    }
//    std::cout<<"size is "<<vec_search_action.size()<<std::endl;
    session_show_click_info_t temp_show_click_info;
    if(0 == calc_one_session(vec_search_action,session_info,temp_show_click_info,conf_info)){
//        std::cout<<"is go to here if not the result will be empty"<<std::endl;
        temp_show_click_info.session_key = session_key;
        session_click_info.push_back(temp_show_click_info);
    }
    return 0;
}

int calc_one_session(vector<search_action_pos_t> & vec_search_action,vector<log_info_t> & session_info,session_show_click_info_t & temp_show_click_info,const conf_info_t & conf_info)
{
    if(0 == vec_search_action.size()){
        return -1;
    }
    vector<search_action_pos_t>::iterator iter;
    for(iter = vec_search_action.begin();iter != vec_search_action.end();++iter){
        int start_pos = iter->start_pos;
        int end_pos = iter->end_pos;
        one_search_info_t temp_one_search_info;
        temp_one_search_info.Reset();
//        std::cout<<"long"<<std::endl;
        if(0 == process_one_search(session_info,start_pos,end_pos,temp_one_search_info,conf_info)){
//            std::cout<<"process_one_search's return is 0"<<std::endl;
            temp_show_click_info.search_click_info.push_back(temp_one_search_info); 
        }
//        std::cout<<"lijian...."<<temp_show_click_info.search_click_info.size()<<std::endl;
    }
    if(0 == temp_show_click_info.search_click_info.size()) {
//        std::cout<<"process_one_search's result is empyt"<<std::endl;
        return 1;
    }//not get usefull info
    return 0;
}

int process_one_search(vector<log_info_t> & session_info, int start ,int end, one_search_info_t & one_search_info,const conf_info_t & conf_info){
    if(start >= end || start >= (int)session_info.size() || end >= (int)session_info.size()){
        return -1;
    }
    vector<log_info_t>::iterator vec_iter;
    //第一条是检索日志，后面是点击日志
    //if(0 == session_info[start].vec_uids.size()){
     //   ul_writelog(UL_LOG_DEBUG,"[%s:%d] this search has no show uids,step:%d",__FILE__,__LINE__,session_info[start].step);
    //    return -1;
    //}
    //copy show result,现在这里要改变的就是把所有的都整合到一起,把所有step为start 的都拷贝到一起
    //copy_show_result(session_info[start],one_search_info);
//    std::cout<<"debug info in process_one_search"<<std::endl;
    copy_show_result(session_info,start,one_search_info);
//    std::cout<<"result_num "<<one_search_info.result_num<<std::endl;
//    for(int i=0;i<one_search_info.result_num;i++){
//        std::cout<<one_search_info.vec_distance[i]<<std::endl;
//    }
    //统计一次search的点击
//    std::cout<<"vec_uids size of one_search_info "<<MAX_SHOW_NUM<<endl;
    for(int i=0;i<MAX_SHOW_NUM;i++){
//        std::cout<<one_search_info.vec_uids[i]<<std::endl; 
    }
    if(0 != count_click_for_one_search(session_info,start,end,one_search_info,conf_info)){
//        std::cout<<"count_click_for_one_search error"<<std::endl;
        ul_writelog(UL_LOG_DEBUG,"[%s:%d] count_click_for_one_search error,start step:%d",__FILE__,__LINE__,session_info[start].step);
        return -1;
    }
    //计算权值
    calc_weighted_click(one_search_info);
    //计算检查概率
    calc_examine_prob(one_search_info);
    //计算消除位置偏向的权值
    calc_weighted_click_of_bias(one_search_info);
    return 0;
}

//not finished very important
int copy_show_result(vector<log_info_t> & session_info,int start ,one_search_info_t & one_search_info){
    vector<log_info_t>::iterator vec_iter;
    bool isfirstmeet = true;
    int show_uid_num = 0;
    bool all_scand = true;
    bool b_has_loc = false;
    for(vec_iter = session_info.begin(); vec_iter != session_info.end();++vec_iter){
        if(vec_iter->step == start){
            ++show_uid_num;
            if(isfirstmeet){
                one_search_info.query_type = vec_iter->query_type;
                one_search_info.query = vec_iter->query;
                one_search_info.city_id = vec_iter->city_id;
                one_search_info.page_num = vec_iter->page_num;
                one_search_info.request_num = vec_iter->request_num;
                one_search_info.bound = vec_iter->bound;
                one_search_info.coordinate = vec_iter->coordinate;
                one_search_info.start_step = vec_iter->step;
                //
                
                if(0 != one_search_info.coordinate.x && 0 != one_search_info.coordinate.y){
                    b_has_loc = true;
                }
                //把每个展现的uid都合并在一个one_search_info中
//                std::cout<<"very very important,vec_uids size "<<vec_iter->vec_uids.size()<<"\tshowuid"<<"\t"<<vec_iter->vec_uids[0]<<std::endl;
                one_search_info.vec_uids[show_uid_num-1] = vec_iter->vec_uids[0];
//                std::cout<<"error place\t"<<one_search_info.vec_uids[show_uid_num-1]<<std::endl;

                if(b_has_loc) {
//                    std::cout<<"pkullj: \t"<<vec_iter->coordinate_result.x<<"\t"<<vec_iter->coordinate_result.y<<std::endl;
                    if(0 != vec_iter->coordinate_result.x && 0 != vec_iter->coordinate_result.y){
                        one_search_info.vec_distance[show_uid_num-1] = calc_dis(one_search_info.coordinate.x,one_search_info.coordinate.y,vec_iter->coordinate_result.x,vec_iter->coordinate_result.y);
                    }
                    else{
                        one_search_info.vec_distance[show_uid_num-1] = -1;
                        //++ m_no_dis_num;
                    }
                }
                else{
                    one_search_info.vec_distance[show_uid_num-1] = -1;
                    //++ m_no_dis_num;
                }
                isfirstmeet = false;
            }
            else{
 //               std::cout<<"very very important,vec_uids size "<<vec_iter->vec_uids.size()<<"showuids\t"<<vec_iter->vec_uids[0]<<std::endl;
                one_search_info.vec_uids[show_uid_num-1] = vec_iter->vec_uids[0];
                if(b_has_loc){
//                    std::cout<<"pkullj: \t"<<vec_iter->coordinate_result.x<<"\t"<<vec_iter->coordinate_result.y<<std::endl;
                    if(0 != vec_iter->coordinate_result.x && 0 != vec_iter->coordinate_result.y){
                        one_search_info.vec_distance[show_uid_num-1] = calc_dis(one_search_info.coordinate.x,one_search_info.coordinate.y,vec_iter->coordinate_result.x,vec_iter->coordinate_result.y);
                    }
                    else{
                        one_search_info.vec_distance[show_uid_num-1] = -1;
                        //++ m_no_dis_num;
                    }
                }
                else{
                    one_search_info.vec_distance[show_uid_num-1] = -1; 
                    //++ m_no_dis_num;
                }
            }
        }
        if((vec_iter->step > start) and all_scand){
                one_search_info.result_num = show_uid_num;
                if(one_search_info.result_num > MAX_SHOW_NUM){
                    one_search_info.result_num = MAX_SHOW_NUM; 
                }
                one_search_info.show_num = one_search_info.result_num;
                all_scand = false;
        }
    }
    return 0;
}

int calc_dis(const double &x1,const double &y1,const double &x2,const double &y2){
    wsl::Point pt1(x1,y1);
    wsl::Point pt2(x2,y2);
    int dis = (int)(wsl::radian::distance(pt1,pt2));
    return dis;
}
//not finish
int calc_weighted_click(one_search_info_t & one_search_info){
    int satisfy_uid_num = 0;
    for(int i = 0;i < one_search_info.result_num; i++){
        if(one_search_info.click_action[i].satisfy_num > 0){
            satisfy_uid_num ++;
        } 
    }
    //统计每个uid的加权点击
    for(int i = 0; i < one_search_info.result_num; i++){
        if(one_search_info.click_action[i].satisfy_num > 0){
            
            one_search_info.weight_examine[i].weight = 6.0/(float)satisfy_uid_num;
            if(one_search_info.weight_examine[i].weight < 2){
                one_search_info.weight_examine[i].weight =2; 
            }
        }
        else if(one_search_info.click_action[i].interest_num > 0){
            one_search_info.weight_examine[i].weight = 1; 
        }
        else if(one_search_info.click_action[i].normal_num > 0){
            if(one_search_info.click_action[i].normal_num >= 2){
            
                one_search_info.weight_examine[i].weight = 1; 
            } 
            else{
                if(one_search_info.show_num > 5){
                    if(0 == i){
                        one_search_info.weight_examine[i].weight = 0.85;
                    } else if(1 == i){
                        one_search_info.weight_examine[i].weight = 0.9;
                    } else {
                        one_search_info.weight_examine[i].weight = 0.95;
                    } 
                }
                else {
                    one_search_info.weight_examine[i].weight = 1;
                }
            }
        }
    }
    return 0;
}
//not finish
int calc_examine_prob(one_search_info_t & one_search_info){
   int clk_uid_num = 0; 

   int last_clk_idx = -1;

   for(int i = 0; i< one_search_info.result_num;i++){
       if(one_search_info.click_action[i].satisfy_num != 0 || one_search_info.click_action[i].normal_num > 0 || one_search_info.click_action[i].interest_num > 0){
            clk_uid_num++;
            last_clk_idx = i;
       }
   }
   int show_num = 0;
   if(0 == one_search_info.acc_uid_num){
       show_num = one_search_info.result_num;
   }else if(one_search_info.hidden_click > 0){
       show_num = one_search_info.result_num;    
   }else{
       show_num = one_search_info.acc_uid_num; 
   }

   for(int i = 0; i < show_num; i++){
        if(i <= last_clk_idx){
            one_search_info.weight_examine[i].examine_p = 0.9; 
        } 
   }

   for(int i= last_clk_idx +1; i < show_num; i++){
       one_search_info.weight_examine[i].examine_p = g_pos_crt[i - last_clk_idx -1] * 0.3;
   }

   if(show_num < 5){
        for(int i = 0; i < show_num ;i++){
            one_search_info.weight_examine[i].examine_p = 0.9;
        } 
   }

   if(one_search_info.hidden_click > 0){
       for(int i = 0; i < one_search_info.acc_uid_num; i++){
           one_search_info.weight_examine[i].examine_p = 1;
       }
   }
   
   for(int i = 0; i < one_search_info.result_num; i++){
        if(one_search_info.click_action[i].satisfy_num !=0 || one_search_info.click_action[i].normal_num > 0 || one_search_info.click_action[i].interest_num > 0){
            one_search_info.weight_examine[i].examine_p = 1;
        } 
   }
   return 0;
}
//not finish
int calc_weighted_click_of_bias(one_search_info_t & one_search_info){
    for(int i = 0; i < one_search_info.result_num;i++){
        if(one_search_info.weight_examine[i].weight != 0){
            one_search_info.weight_examine[i].click_num = 1;
        } 
        if((one_search_info.acc_uid_num > 0 )&&(i >= one_search_info.acc_uid_num)){
            one_search_info.weight_examine[i].weighted_pos_clk = one_search_info.weight_examine[i].weight * HIDDEN_BIAS; 
        }
        else{
            one_search_info.weight_examine[i].weighted_pos_clk = one_search_info.weight_examine[i].weight / g_pos_crt[i];
        }
    }
    return 0;
}

//not finish
int count_click_for_one_search(vector<log_info_t> & session_info,int start,int end,one_search_info_t & one_search_info,const conf_info_t & conf_info){
    if(start >= end || start >= (int)session_info.size() || end >= (int)session_info.size()){
        return -1;
    }
    int kdx = 0;
    int ret = 1;
    int index = 0;
    vector<log_info_t>::iterator vec_iter;
    for(vec_iter = session_info.begin();vec_iter != session_info.end();++vec_iter,++index){
        if(vec_iter->step <= start){
//            std::cout<<"one"<<std::endl;
            continue;
        }
        if(vec_iter->step > end){
//            std::cout<<"two"<<std::endl;
            continue;
        }
        if(0 == vec_iter->click_uid){
//            std::cout<<"three"<<std::endl;
            continue;
        }
        for(kdx = 0;kdx < one_search_info.result_num; ++kdx){
//            std::cout<<vec_iter->click_uid<<"\t"<<vec_iter->action_id<<"\t"<<one_search_info.vec_uids[kdx]<<std::endl;
            if(vec_iter->click_uid == one_search_info.vec_uids[kdx]){
                break;
            } 
        }
        if(kdx == one_search_info.result_num){
            //++ m_click_not_in_show 
//            std::cout<<"four"<<std::endl;
            continue;
        }
        //记录点击动作
        int click_class = get_click_class(vec_iter->action_id,conf_info.satisfy_action_ids,conf_info.interest_action_ids);
        if(-1 == click_class){
            //++ invalid_click_num;
//            std::cout<<"five"<<std::endl;
            continue;
        }
        ret = 0;
        switch(click_class){
            case CLICK_NORMAL_CLASS:
                one_search_info.click_action[kdx].normal_num ++;
                break;
            case CLICK_INTEREST_CLASS:
                one_search_info.click_action[kdx].interest_num ++;
                break;
            case CLICK_SATISFY_CLASS:
                one_search_info.click_action[kdx].satisfy_num ++;
                break;
        }
    }
    return ret;
}
//trans_session to uid and output 
int output_session_click_info(list<session_show_click_info_t> & session_click_info){
    if(0 == session_click_info.size()){
        ul_writelog(UL_LOG_FATAL,"[%s:%d] session_click_info is empyt",__FILE__,__LINE__);
        return -1;
    }
    list<session_show_click_info_t>::iterator list_iter;
    vector<one_search_info_t>::iterator vec_iter;
    //output 
    for(list_iter = session_click_info.begin(); list_iter != session_click_info.end();++list_iter){
        for(vec_iter = list_iter->search_click_info.begin(); vec_iter != list_iter->search_click_info.end();++vec_iter){
            for(int idx = 0; idx < vec_iter->result_num; ++idx){
                if(0 != vec_iter->weight_examine[idx].weighted_pos_clk || 0 != vec_iter->weight_examine[idx].examine_p){
                    uid_one_time_info_t temp_one_uid_info;
                    temp_one_uid_info.Reset();
                    uint64_t uid = vec_iter->vec_uids[idx];
                    if(uid == 18446744073709551615){
                        ul_writelog(UL_LOG_FATAL,"[%s:%d] cuid:%s,session_id:%s,step:%d!",__FILE__,__LINE__,list_iter->session_key.cuid.c_str(),list_iter->session_key.session_id.c_str(),vec_iter->start_step);
                        continue;
                    }
                    temp_one_uid_info.cid = vec_iter->city_id;
                    temp_one_uid_info.click_weight = vec_iter->weight_examine[idx];
                    temp_one_uid_info.distance = vec_iter->vec_distance[idx];
                    temp_one_uid_info.page_num= vec_iter->page_num;
                    temp_one_uid_info.position= idx;
                    temp_one_uid_info.query = vec_iter->query;
                    if(0 == vec_iter->weight_examine[idx].weighted_pos_clk){
                        temp_one_uid_info.page_num= 0;
                        temp_one_uid_info.position= 0;
                    }
                    //output format not complete
                    std::cout<<uid<<"\t"<<temp_one_uid_info.query<<"\t";
                    std::cout<<temp_one_uid_info.cid<<"\t";
                    std::cout<<temp_one_uid_info.click_weight.weight<<"\t";
                    std::cout<<temp_one_uid_info.click_weight.examine_p<<"\t";
                    std::cout<<temp_one_uid_info.click_weight.weighted_pos_clk<<"\t";
                    std::cout<<temp_one_uid_info.click_weight.click_num<<"\t";
                    std::cout<<temp_one_uid_info.page_num<<"\t";
                    std::cout<<temp_one_uid_info.position<<"\t";
                    std::cout<<temp_one_uid_info.distance<<std::endl;
                } 
            }
        } 
    }
    return 0;
}

int init_action_ids(set<uint64_t> & satisfy_action_ids,set<uint64_t>  & interest_action_ids){
    std::ifstream satisfy_in;
    std::ifstream interest_in;
    satisfy_in.open("../conf/satisfy_action_ids",ios::in);
    interest_in.open("../conf/interest_action_ids",ios::in);
    std::string satisfy_line,interest_line;
    while(!satisfy_in.eof()){
        std::getline(satisfy_in,satisfy_line);
        if(satisfy_line != ""){
            uint64_t search_id = strtoul(satisfy_line.c_str(),NULL,10);
            if(satisfy_action_ids.end() == satisfy_action_ids.find(search_id)){
                satisfy_action_ids.insert(search_id); 
            }
        }
    }
    satisfy_in.close();
    while(!interest_in.eof()){
        std::getline(interest_in,interest_line);
        if(interest_line != ""){
            uint64_t search_id = strtoul(interest_line.c_str(),NULL,10);
            if(interest_action_ids.end() == interest_action_ids.find(search_id)){
                interest_action_ids.insert(search_id); 
            }
        }
    }
    interest_in.close();
    return 0;
}

int main(int argc,char **argv,char **env)
{
    conf_info_t conf_info;
    map<session_key_t,vector<log_info_t> > map_raw_log;
    list<session_show_click_info_t> session_click_info;
    set<uint64_t> satisfy_action_ids;
    set<uint64_t> interest_action_ids;
    if(0 != init_action_ids(conf_info.satisfy_action_ids,conf_info.interest_action_ids)){
        ul_writelog(UL_LOG_FATAL,"[%s:%d] init_action_ids error!",__FILE__,__LINE__);
        return -1;
    }
   // char filename[9];
   // for(int i=0;env[i] != NULL;i++){
   //     if(strncmp(env[i],"filename=",9) == 0){
   //        snprintf(conf_info.cur_process_file_name,9,"%s",env[i]);
   //     } 
   // }
   // std::cout<<"hello"<<std::endl;
   // std::cout<<conf_info.cur_process_file_name<<std::endl;
//    //导入配置文件
    if(0 != load_conf(conf_info)){
        ul_writelog(UL_LOG_FATAL,"[%s:%d] load_conf error!",__FILE__,__LINE__);
        return -1;
    }
    //先一行一行的读取标准输入中的数据，这里就是一天的日志数据
    int error_line_num = 0;
    int ret = 0;
//    std::cout<<E_TOTAL_FIELD_NUM<<std::endl;
//    std::cout<<E_click_loc<<std::endl;
    for(std::string line; std::getline(std::cin,line);){
       // std::cout<<line<<endl;
        vector<string> vec_buf;
        SplitItems(line,"\t",vec_buf);
        if(vec_buf.size() < E_click_loc){
            ++error_line_num;
            ul_writelog(UL_LOG_WARNING,"[%s:%d] bufsize:%lu,buf:%s,process error!",__FILE__,__LINE__,vec_buf.size(),line.c_str());
            continue;
        }
        //把每一天的数据转换为map_raw_log
        ret = trans_raw2info(conf_info,vec_buf,map_raw_log);
        if(ret < 0){
//            std::cout<<"trans_raw2info error\n";
            ul_writelog(UL_LOG_WARNING,"[%s:%d] transbuf:%s process error!",__FILE__,__LINE__,line.c_str());
        }
        else if(ret == 0){
//            std::cout<<"trans_raw2info ok\n";
            //store usefule session data
        }
    }
    ul_writelog(UL_LOG_NOTICE,"[%s:%d] error line num %d!",__FILE__,__LINE__,error_line_num);
    //std::cout<<map_raw_log.size()<<std::endl;
    //CProcessDayData process_day_data(&conf_info);

    //打印输出结果,现在修改一
    map<session_key_t,vector<log_info_t> >::iterator iter;
    for(iter = map_raw_log.begin(); iter != map_raw_log.end();++iter){
        sort(iter->second.begin(),iter->second.end(),log_info_t::sort_by_step);
        //std::cout<<iter->first.session_id<<std::endl;
        process_one_session(iter->first,iter->second,session_click_info,conf_info); 
    }
    //打印输出结果
    //output session_click_info
//    std::cout<<"output size "<<session_click_info.size()<<std::endl;
    output_session_click_info(session_click_info);
    return 0;
}
