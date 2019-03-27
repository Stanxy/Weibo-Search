import Embeddingtools as ebt
import numpy as np

class Dataset_constructors:

    def __init__(self):
        pass

    def query_history_embedding(self, query_history_list, w2v, ineffective_cases, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
        '''
        Create the list of uid, history embedding pairs
        
        Arguments:
        query_history_list -- the list uid_query tuples [(uid, q1, q2, ..., qn)]
        w2v -- word to vec transfer object
        ineffective_uids -- the ineffective uids that do not has complete featrues, a set.

        top_k_importance -- the top k words analyzed by jieba after removing stopwords and applying tf-idf filter
        star_pool -- star name set that used to tract stars from the tags, dtype: list( Preparing )
        event_pool -- event that used to tract events from the tags, dtype: list( Preparing )
        
        Returns:
        query_history_embedding_list -- the list of user,query history embedding tuple, e.g. [(uid, embedding vector)]
        '''

        query_history_embedding_list = []
        case_num = 0
        for case in query_history_list:
            if case_num not in ineffective_cases:
                #print(case)
                for query_order in range(1,len(case)):
                    embedding_vec = ebt.phrase_embedding(case[query_order], w2v, top_k_importance, filter, star_pool, event_pool)

                    if query_order == 1:
                        cum_embedding = embedding_vec
                    else:
                        cum_embedding += embedding_vec
                ave_embedding = cum_embedding/(len(case)-1)
                
                query_history_embedding_list.append(tuple([case[0]] + [ave_embedding]))
                case_num += 1
            else:
                case_num += 1
                continue

        return query_history_embedding_list

    def recommend_embedding(self, recommend_tuple_list, w2v, ineffective_cases, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
        '''
        Construct the list of recommend pairs. Here the input is the list of tuples 
        Arguments:
        recommend_tuple_list -- the list of user recommend phrase tuple, 
        [(uid, date, time, phrase, recommend1, recommend2, ..., recommend_{negative_samples} )]

        Returns:
        recommend_embedding_list -- the list of user, recommend embedding tuple, e.g. [(uid, embedding1, embedding2, ..., embedding_n)]
        '''
        recommend_embedding_list = []
        case_num = 0
        for case in recommend_tuple_list:
            if case_num not in ineffective_cases:
                embedded = []
                for query_order in range(4,len(case)):
                    embedding_vec = ebt.phrase_embedding(case[query_order], w2v, top_k_importance, filter, star_pool, event_pool)
                    embedded.append(embedding_vec)
                recommend_embedding_list.append(tuple([case[0]] + embedded))
                case_num += 1
            else:
                case_num += 1
                continue

        return recommend_embedding_list

    def query_embedding(self, query_tuple_list, w2v, ineffective_cases, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
        '''
        Construct the list of query pairs. Here the input is the list of tuples 
        Arguments:
        query_tuple_list -- the list of user query phrase tuple, 
        [(date, time, phrase, uid, hebavior )]

        Returns:
        query_embedding_list -- the list of user, query embedding tuple, e.g. [(uid, embedding)]
        '''
        query_embedding_list = []
        case_num = 0
        for case in query_tuple_list:
            if case_num not in ineffective_cases:
                embedding_vec = ebt.phrase_embedding(case[2], w2v, top_k_importance, filter, star_pool, event_pool)
                query_embedding_list.append(tuple([case[3]]+ [embedding_vec]))
                case_num += 1
            else:
                case_num += 1
                continue
        return query_embedding_list

    
    def hotphrase_filter(self, hotphrase_tuple_list, ineffective_cases):
        '''
        Construct the list of hotphrase embedding pairs. Here the input is the list of tuples 
        
        Arguments:
        hotphrase_tuple_list -- the list of user hotphrase phrase tuple, 
        [(uid, date, time, phrase, hotphrase1, hotphrase2, ..., hotphrase50)]

        Returns:
        hotphrase_filtered_list -- the list of user, hotphrase filtered tuple, e.g. [(uid, hotphrase1, ... , hotprasen)]
        '''

        hotphrase_filtered_list = []
        case_num = 0    
        for case in hotphrase_tuple_list:
            if case_num not in ineffective_cases:
                hotphrase_filtered_list.append(tuple([case[0]] + list(case[3:])))
                case_num += 1
            else:
                case_num += 1
                continue
        return hotphrase_filtered_list

    def hotphrase_embedding(self, hotphrase_tuple_list, w2v, ineffective_cases, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
        '''
        Construct the list of hotphrase embedding pairs. Here the input is the list of tuples 
        
        Arguments:
        hotphrase_tuple_list -- the list of user hotphrase phrase tuple, 
        [(uid, date, time, phrase, hotphrase1, hotphrase2, ..., hotphrase50)]

        Returns:
        hotphrase_filtered_list -- the list of user, hotphrase filtered tuple, e.g. [(uid, hpebd1, ... , hpebdn)]
        '''

        hotphrase_embedding_list = []
        case_num = 0
        for case in hotphrase_tuple_list:
            if case_num not in ineffective_cases:
                phrase_ebd = []
                for phrase in case[4:]:
                    embedding_vec = ebt.phrase_embedding(phrase, w2v, top_k_importance, filter, star_pool, event_pool)
                    phrase_ebd.append(embedding_vec)
                hotphrase_embedding_list.append(tuple([case[0]]+ phrase_ebd))
                case_num += 1
            else:
                case_num += 1
                continue
        return hotphrase_embedding_list

    def generate_labels(self,hotphrase_embedding_list, query_embedding_list):
        '''Testing func'''
        labels_list = []
        for i in range(len(hotphrase_embedding_list)):
            try:
                assert hotphrase_embedding_list[i][0] == query_embedding_list[i][0]
            except:
                print('uid not match!')
                break
            hot_phrase = list(hotphrase_embedding_list[i][1:])
            query = query_embedding_list[i][1]
            labels = np.zeros((len(hot_phrase),1))
            for j in range(len(hot_phrase)):
                if sum(sum(query == hot_phrase[j])) == query.shape[0]:
                    labels[j] = 1
                else:
                    continue
            labels_list.append(tuple([hotphrase_embedding_list[i][0]]+[labels]))
        
        return labels_list


    def user_interest_filter(self, interest_tuple_list, ineffective_cases):
        '''
        Construct the list of interest pairs. Here the input is the list of tuples 
        
        Arguments:
        interest_tuple_list -- the list of user interest phrase tuple, 
        [(uid, interest1, interest2, ..., interest25)]

        Returns:
        interest_filtered_list -- the list of user, interest filtered tuple, e.g. [(uid, interest1, ... , interestn)]
        '''

        interest_filtered_list = []
        case_num = 0    
        for case in interest_tuple_list:
            if case_num not in ineffective_cases:
                interest_filtered_list.append(case)
                case_num += 1
            else:
                case_num += 1
                continue
        return interest_filtered_list

    def recommend_filter(self, recommend_tuple_list, ineffective_cases):
        '''
        Construct the list of recommend phrase pairs. Here the input is the list of tuples

        Arguments:
        recommend_tuple_list -- the list of user recommend phrase tuple,
        [(uid, interest1, interest2, ..., interestn)]

        Returns:
        recommend_filtered_list -- the list of user, recommend filtered tuple, e.g. [(uid, interest1, interest2, ..., interestn)]
        '''

        recommend_filtered_list = []
        case_num = 0
        for case in recommend_filtered_list:
            if case_num not in ineffective_cases:
                recommend_filtered_list.append(tuple([case[0]] + list(case[4:])))
                case_num += 1
            else:
                case_num += 1
                continue
        return recommend_filtered_list
        
    
    def training_set_constructor(self, embedded_query_history, user_interest, query_embedding, labels_list, hotphrase_embeddings):
        '''
        Construct the training set of Deep Neural Network. Here the input is the list of tuples

        Arguments:
        embedded_query_history -- the user query history embedding list, [(uid, embedding)]
        user_interest -- the user interest list , [(uid, interest1, interest2, ..., interest 25)]
        query_embedding -- the embedding of users query, [(uid, embedding)]
        labels_list -- the embedding of recommended history, [(uid, label_array[[],[],[]...])]
        hotphrase_embeddings -- the user hot phrases [(uid, embedding1, embedding2, ..., embedding_n)]

        Returns:
        input_X1 -- input, np.array of shape (m, embedding_len)
        input_X2 -- input, np.array of shape (m, interest_len) 
        input_Y -- input, np.array of shape (m, 1)
        '''
        #ndcg_sample = []
        for i in range(len(query_embedding)):
            try:
                assert embedded_query_history[i][0] == user_interest[i][0]
                assert embedded_query_history[i][0] == query_embedding[i][0]
                assert embedded_query_history[i][0] == labels_list[i][0]
                assert embedded_query_history[i][0] == hotphrase_embeddings[i][0]
            except:
                print(i)
                print('uid mismatch!')
                
                break

            for j in range(1,len(hotphrase_embeddings[i])):

                interest = np.array(user_interest[i][1:]).reshape(1,-1)
                if i == 0 and j == 1:
                    input_X2 = interest
                else:
                    #print(input_X2)
                    input_X2 = np.concatenate((input_X2,interest), axis = 0)

                hp_ebd = hotphrase_embeddings[i][j].reshape(1,-1)
                if i == 0 and j == 1:
                    input_X1 = hp_ebd
                else: 
                    input_X1 = np.concatenate((input_X1,hp_ebd), axis = 0)
                
            labels = labels_list[i][1].reshape(-1,1)
            if i == 0:
                input_Y = labels
            else: 
                input_Y = np.concatenate((input_Y,labels), axis = 0)

            history = embedded_query_history[i][1].reshape(1,-1)
            input_X2 = np.concatenate((input_X2,interest), axis = 0)
            input_X1 = np.concatenate((input_X1,history), axis = 0)
            input_Y = np.concatenate((input_Y,np.array([[1]])), axis = 0)
        
        return input_X1, input_X2, input_Y    

    '''
    def training_set_constructor(self, embedded_query_history, user_interest, query_embedding, recommend_embedding, hotphrase_tuples):
        '''
'''
        Construct the training set of Deep Neural Network. Here the input is the list of tuples

        Arguments:
        embedded_query_history -- the user query history embedding list, [(uid, embedding)]
        user_interest -- the user interest list , [(uid, interest1, interest2, ..., interest 25)]
        query_embedding -- the embedding of users query, [(uid, embedding)]
        recommend_embedding -- the embedding of recommended history, [(uid, embedding1, embedding2, ..., embedding_n)]
        hotphrase_tuples -- the user hot phrases [(uid, phrase, hotphrase1, hotphrase2, ..., hotphrase50)]

        Returns:
        input_X1 -- input, np.array of shape (m, embedding_len)
        input_X2 -- input, np.array of shape (m, interest_len) 
        input_Y1 -- input, np.array of shape (m, embedding_len)
        input_Y2 -- input, np.array of shape (m, embedding_len)
        ndcg_sample -- a corresponding NDCG sample for evaluating, 
                    this is used to see the performance on the training set. 
                    a list of tuples
        ''' '''
        ndcg_sample = []
        for i in range(len(query_embedding)):
            try:
                assert embedded_query_history[i][0] == user_interest[i][0]
                assert embedded_query_history[i][0] == query_embedding[i][0]
                assert embedded_query_history[i][0] == recommend_embedding[i][0]
                assert embedded_query_history[i][0] == hotphrase_tuples[i][0]
            except:
                print(i)
                print('uid mismatch!')
                
                break

            for j in range(1,len(recommend_embedding[i])):

                interest = np.array(user_interest[i][1:]).reshape(1,-1)
                if i == 0 and j == 1:
                    input_X2 = interest
                else:
                    #print(input_X2)
                    input_X2 = np.concatenate((input_X2,interest), axis = 0)

                history = embedded_query_history[i][1].reshape(1,-1)
                if i == 0 and j == 1:
                    input_X1 = history
                else:
                    input_X1 = np.concatenate((input_X1, history), axis = 0)

                recommend = recommend_embedding[i][j].reshape(1,-1)
                if i == 0 and j == 1:
                    input_Y2 = recommend
                else: 
                    input_Y2 = np.concatenate((input_Y2,recommend), axis = 0)
                
                query = query_embedding[i][1].reshape(1,-1)
                if i == 0 and j == 1:
                    input_Y1 = query
                else: 
                    input_Y1 = np.concatenate((input_Y1,query), axis = 0)

                selected = []
                selected.append(hotphrase_tuples[i][1])
                candidate_list = list(hotphrase_tuples[i][2:])
                ndcg_sample.append([selected,candidate_list])
        
        return input_X1, input_X2, input_Y1, input_Y2, ndcg_sample    
'''

'''
    def testing_set_constructor(self, embedded_query_history, user_interest, hotphrase_tuples, rcmd = False, recommend_phrase = None):
        ''' '''
        Construct the testing set of Deep Neural Network. Here the input is the list of tuples

        Arguments:
        embedded_query_history -- the user query history embedding list, [(uid, embedding)]
        user_interest -- the user interest list , [(uid, emb_interest1, emb_interest2, ..., emb_interest 25)]
        hotphrase_tuples -- the user hot phrases [(uid, phrase, hotphrase1, hotphrase2, ..., hotphrase50)]
        rcmd -- a flag to control whether you need to see the top 6 recommended word of each case.
        recommend_phrase -- the list of recommended phrase tuples, avliable when rcmd is true. 
                            [(uid, recommend1, recommend2, ..., recommend_{negative_samples} )]
        
        Returns:
        input_X1 -- input, np.array of shape (m, embedding_len)
        input_X2 -- input, np.array of shape (m, interest_len) 

        ndcg_sample -- a corresponding NDCG sample for evaluating, 
                    this is used to see the performance on the training set. 
                    a list of tuples
        recommend_list -- the recommended phrases of the samples we chose, 
                    this is used to calculate the similarity of this model compared with the last model
        '''
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
                #print(input_X2)
                input_X2  = np.concatenate((input_X2, interest), axis = 0)
        
            history = embedded_query_history[i][1].reshape(1,-1)
            if i == 0:
                input_X1 = history
            else:
                input_X1  = np.concatenate((input_X1, history), axis = 0)

            if rcmd:
                recommend = list(recommend_phrase[i][1:])
                recommend_list.append(recommend)

                selected = []
                selected.append(hotphrase_tuples[i][1])
                candidate_list = list(hotphrase_tuples[i][2:])
                ndcg_sample.append([selected,candidate_list])
                return input_X1, input_X2, ndcg_sample, recommend_list

            selected = []
            selected.append(hotphrase_tuples[i][1])
            candidate_list = list(hotphrase_tuples[i][2:])
            ndcg_sample.append([selected,candidate_list])
        
        return input_X1, input_X2, ndcg_sample
        '''