import SQLtools.SQL_out as SQL_out
import Embeddingtools

 
class Dataset_constructors:

    def __init__(self, path):
        self.handle = SQL_out(path)

    def user_history_embedding(self,query_tuple_list = self.uid_query_tuples ,word_to_vec_path, stop_word_path, dict_path_list, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
        '''
        Create the list of uid, embedding pairs
        
        Argument:
        query_tuple_list -- the list uid_query tuples

        word_to_vec_path -- the path of the word_to_vec bin file, dtype: string
        stop_word_path -- the path of the stop word dictionary, dtype: string
        dict_path_list -- the list of all the path of the dictionarys needed, dtype: string

        top_k_importance -- the top k words analyzed by jieba after removing stopwords and applying tf-idf filter
        star_pool -- star name set that used to tract stars from the tags, dtype: list( Preparing )
        event_pool -- event that used to tract events from the tags, dtype: list( Preparing )
        
        Return:
        embedding_tuple_list -- the list of user,query history embedding tuple, e.g. [(uid, embedding vector)]
        '''
        w2v, flag = embedding_config(word_to_vec_path, stop_word_path, dict_path_list)

        embedding_tuple_list = []
        for case in query_tuple_list:
            for query_order in range(1,len(case)):
                embedding_vec = phrase_embedding(case[query_order], w2v, top_k_importance, filter, star_pool, event_pool)
                
                if query_order == 1:
                    cum_embedding = embedding_vec
                else:
                    cum_embedding += embedding_vec
            ave_embedding = cum_embedding/(len(case)-1)
            embedding_tuple_list.append(tuple([case[0], ave_embedding]))

        return embedding_tuple_list