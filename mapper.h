#ifndef  __MAPPER_H_
#define  __MAPPER_H_

int dispatch_session(vector<log_info_t> & session_info,vector<search_action_pos_t> & vec_search_action,const conf_info_t & conf_info);

int process_one_session(const session_key_t & session_key,vector<log_info_t> & session_info,list<session_show_click_info_t> & session_click_info,const conf_info_t & conf_info);

int calc_one_session(vector<search_action_pos_t> & vec_search_action,vector<log_info_t> & session_info,session_show_click_info_t & temp_show_click_info,const conf_info_t & conf_info);

int process_one_search(vector<log_info_t> & session_info,int start,int end,one_search_info_t & one_search_info,const conf_info_t & conf_info);

int copy_show_result(vector<log_info_t> & session_info,int start,one_search_info_t & one_search_info);

int count_click_for_one_search(vector<log_info_t> & session_info,int start,int end,one_search_info_t & one_search_info,const conf_info_t & conf_info);

int calc_weighted_click(one_search_info_t & one_search_info);

int calc_examine_prob(one_search_info_t & one_search_info);

int calc_weighted_click_of_bias(one_search_info_t & one_search_info);

int calc_dis(const double &x1,const double &y1,const double &x2,const double &y2);

int get_click_class(uint64_t action_id,const set<uint64_t> &,const set<uint64_t> &);

int init_action_ids(set<uint64_t> & ,set<uint64_t> &);

#endif  //__MAPPER_H_

