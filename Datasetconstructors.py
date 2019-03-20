import Embeddingtools as ebt
import numpy as np

class Dataset_constructors:

    def __init__(self):
        pass

    def query_history_embedding(self, query_history_list, w2v, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
        '''
        Create the list of uid, history embedding pairs
        
        Argument:
        query_history_list -- the list uid_query tuples [(uid, q1, q2, ..., qn)]

        top_k_importance -- the top k words analyzed by jieba after removing stopwords and applying tf-idf filter
        star_pool -- star name set that used to tract stars from the tags, dtype: list( Preparing )
        event_pool -- event that used to tract events from the tags, dtype: list( Preparing )
        
        Return:
        query_history_embedding_list -- the list of user,query history embedding tuple, e.g. [(uid, embedding vector)]
        '''

        query_history_embedding_list = []
        for case in query_history_list:
            for query_order in range(1,len(case)):
                embedding_vec = ebt.phrase_embedding(case[query_order], w2v, top_k_importance, filter, star_pool, event_pool)
                
                if query_order == 1:
                    cum_embedding = embedding_vec
                else:
                    cum_embedding += embedding_vec
            ave_embedding = cum_embedding/(len(case)-1)
            query_history_embedding_list.append(tuple([case[0], ave_embedding]))

        return query_history_embedding_list

    def recommend_embedding(self, recommend_tuple_list, w2v, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
        '''
        Construct the list of recommend pairs. Here the input is the list of tuples 
        Arguments:
        recommend_tuple_list -- the list of user recommend phrase tuple, 
        [(uid, date, time, phrase, recommend1, recommend2, ..., recommend_{negative_samples} )]

        Return:
        recommend_embedding_list -- the list of user, recommend embedding tuple, e.g. [(uid, embedding1, embedding2, ..., embedding_n)]
        '''
        recommend_embedding_list = []
        for case in recommend_tuple_list:
            embedded = []
            for query_order in range(4,len(case)):
                embedding_vec = ebt.phrase_embedding(case[query_order], w2v, top_k_importance, filter, star_pool, event_pool)
                embedded.append(embedding_vec)
            recommend_embedding_list.append(tuple(list(case[0]) + embedded))

        return recommend_embedding_list

    def query_embedding(self, query_tuple_list, w2v, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
        '''
        Construct the list of query pairs. Here the input is the list of tuples 
        Arguments:
        query_tuple_list -- the list of user query phrase tuple, 
        [(date, time, phrase, uid, hebavior )]

        Return:
        query_embedding_list -- the list of user, query embedding tuple, e.g. [(uid, embedding)]
        '''
        query_embedding_list = []
        for case in query_tuple_list:
            embedding_vec = ebt.phrase_embedding(case[2], w2v, top_k_importance, filter, star_pool, event_pool)
            query_embedding_list.append(tuple([case[0], embedding_vec]))

        return query_embedding_list

    '''
    def horphrase_embedding(self, hotphrase_tuple_list, w2v, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
        
        #Construct the list of hotphrase embedding pairs. Here the input is the list of tuples 
        #Arguments:
        #hotphrase_tuple_list -- the list of user hotphrase phrase tuple, 
        #[(uid, date, time, phrase, hotphrase1, horphrase2, ..., hotphrase50)]

        #Return:
        #hotphrase_embedding_list -- the list of user, hotphrase embedding tuple, e.g. [(uid, embedding1, embedding2, ...)]
        
        hotphrase_embedding_list = []
        for case in hotphrase_tuple_list:
            embedded = []
            for query_order in range(4,len(case)):
                embedding_vec = phrase_embedding(case[query_order], w2v, top_k_importance, filter, star_pool, event_pool)
                embedded.append(embedding_vec)
            hotphrase_embedding_list.append(tuple(list(case[0]) + embedded))

        return hotphrase_embedding_list
    '''
    
    def training_set_constructor(self, embedded_query_history, user_interest, query_embedding, recommend_embedding, hotphrase_tuples):
        '''
        Construct the training set of Deep Neural Network. Here the input is the list of tuples

        Arguments:
        embedded_query_history -- the user query history embedding list, [(uid, embedding)]
        user_interest -- the user interest list , [(uid, emb_interest1, emb_interest2, ..., emb_interest 25)]
        query_embedding -- the embedding of users query, [(uid, embedding)]
        recommend_embedding -- the embedding of recommended history, [(uid, embedding1, embedding2, ..., embedding_n)]
        hotphrase_tuples -- the user hot phrases [(uid, date, time, phrase, hotphrase1, horphrase2, ..., hotphrase50)]

        Return:
        input_X1 -- input, np.array of shape (m, embedding_len)
        input_X2 -- input, np.array of shape (m, interest_len) 
        input_Y1 -- input, np.array of shape (m, embedding_len)
        input_Y2 -- input, np.array of shape (m, embedding_len)
        ndcg_sample -- a corresponding NDCG sample for evaluating, 
                    this is used to see the performance on the training set. 
                    a list of tuples
        '''
        ndcg_sample = []
        for i in range(len(user_interest)):
            try:
                assert embedded_query_history[i][0] == user_interest[i][0]
                assert embedded_query_history[i][0] == query_embedding[i][0]
                assert embedded_query_history[i][0] == recommend_embedding[i][0]
                assert embedded_query_history[i][0] == hotphrase_tuples[i][0]
            except:
                print('uid mismatch!')
                break

            for j in range(1,len(user_interest)[i]):

                interest = np.array(user_interest[i][1:]).reshape(1,-1)
                if i == 0 and j == 0:
                    input_X2 = interest
                else:
                    input_X2 = np.concatenate((input_X2, interest), axis = 0)

                history = embedded_query_history[i][1].reshape(1,-1)
                if i == 0 and j == 0:
                    input_X1 = history
                else:
                    input_X1 = np.concatenate((input_X1, history), axis = 0)

                recommend = recommend_embedding[i][j].reshape(1,-1)
                if i == 0 and j == 0:
                    input_Y2 = recommend
                else: 
                    input_Y2 = np.concatenate((input_Y2,recommend), axis = 0)
                
                query = query_embedding[i][1].reshape(1,-1)
                if i == 0 and j == 0:
                    input_Y1 = query
                else: 
                    input_Y1 = np.concatenate((input_Y1,query), axis = 0)

                selected = []
                selected.append(hotphrase_tuples[i][3])
                candidate_list = list(hotphrase_tuples[i][4:])
                ndcg_sample.append([selected,candidate_list])
        
        return input_X1, input_X2, input_Y1, input_Y2, ndcg_sample    

    def testing_set_constructor(self, embedded_query_history, user_interest, recommend_phrase, hotphrase_tuples):
        '''
        Construct the testing set of Deep Neural Network. Here the input is the list of tuples

        Arguments:
        embedded_query_history -- the user query history embedding list, [(uid, embedding)]
        user_interest -- the user interest list , [(uid, emb_interest1, emb_interest2, ..., emb_interest 25)]
        recommend_phrase -- the phrase of recommended history, 
                            [(uid, date, time, phrase, recommend1, recommend2, ..., recommend_{negative_samples} )]
        hotphrase_tuples -- the user hot phrases [(uid, date, time, phrase, hotphrase1, horphrase2, ..., hotphrase50)]

        Return:
        input_X1 -- input, np.array of shape (m, embedding_len)
        input_X2 -- input, np.array of shape (m, interest_len) 
        recommend_list -- the recommended phrases of the samples we chose, 
                        this is used to calculate the similarity of this model compared with the last model
        ndcg_sample -- a corresponding NDCG sample for evaluating, 
                    this is used to see the performance on the training set. 
                    a list of tuples
        '''
        ndcg_sample = []
        recommend_list = []
        for i in range(len(user_interest)):
            try:
                assert embedded_query_history[i][0] == user_interest[i][0]
                assert embedded_query_history[i][0] == recommend_phrase[i][0]
                assert embedded_query_history[i][0] == hotphrase_tuples[i][0]
            except:
                print('uid mismatch!')
                break

            interest = np.array(user_interest[i][1:]).reshape(1,-1)
            if i == 0:
                input_X2 = interest
            else:
                input_X2 = np.concatenate((input_X2, interest), axis = 0)
        
            history = embedded_query_history[i][1].reshape(1,-1)
            if i == 0:
                input_X1 = history
            else:
                input_X1 = np.concatenate((input_X1, history), axis = 0)
            
            recommend = recommend_phrase[i][4:].reshape(1,-1)
            recommend_list.append(recommend)

            selected = []
            selected.append(hotphrase_tuples[i][3])
            candidate_list = list(hotphrase_tuples[i][4:])
            ndcg_sample.append([selected,candidate_list])
        
        return input_X1, input_X2, recommend_list, ndcg_sample