#ifndef  __REDUCER_H_
#define  __REDUCER_H_

int print_map_uid_data(map<uid_query_t,vector<uid_one_time_info_t> > &);

int process_map_uid_query(map<uid_query_t,vector<uid_one_time_info_t> > & uid_query_infos);

int process_uid_of_one_query(const uid_query_t & uid_query,vector<uid_one_time_info_t> & vec_uid_infos);

int calc_uid_query_dis_hot(weight_hot_t *weight_hot,int size);

int output_hot_values(const uid_query_t & uid_query,weight_hot_t *weight_hot,int size);

#endif  //__REDUCER_H_
