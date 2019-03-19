import sqlite3
import Embeddingtools
from tqdm import tqdm
import re

class SQL_in:

    def __init__(self,path):
        '''
        construct function.
        
        Path is the path of the sqlite database, string.
        After this we shall use self.cur to connect the sqlite database
        '''
        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()


    def __del__(self):
        '''
        destructor function
        
        We disconnect with the database
        '''
        self.conn.commit()
        self.cur.close()

    def connect(self,path):
        '''
        path is the path of the sqlite database, string.
        after this we shall use self.cur to connect the sqlite database
        '''
        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()
    
    def disconnect(self):
        self.conn.commit()
        self.cur.close()


    def initialize_db(self,user_table_command):
        '''
        function used to initialize the database 
        '''
        self.cur.executescript('''
        DROP TABLE IF EXISTS Query;
        DROP TABLE IF EXISTS Recommender;
        DROP TABLE IF EXISTS Hotphrase;
        DROP TABLE IF EX

        CREATE TABLE Query (
            date DATE NOT NULL,
            time TIME NOT NULL,
            phrase TEXT,
            uid INTEGER,
            behavior INTEGER,
            util TEXT
        );

        CREATE TABLE Recommender (
            date DATE NOT NULL,
            time TIME NOT NULL,
            uid INTEGER,
            most_hot TEXT NOT NULL,
            special_push TEXT NOT NULL
            recommend1 TEXT NOT NULL,
            recommend2 TEXT NOT NULL,
            recommend3 TEXT NOT NULL,
            recommend4 TEXT NOT NULL,
            recommend5 TEXT NOT NULL,
            recommend6 TEXT NOT NULL
        );

        CREATE TABLE Hotphrase(
            date DATE NOT NULL,
            time TIME NOT NULL,
            hotphrase1 TEXT NOT NULL,
            hotphrase2 TEXT NOT NULL,
            hotphrase3 TEXT NOT NULL,
            hotphrase4 TEXT NOT NULL,
            hotphrase5 TEXT NOT NULL,
            hotphrase6 TEXT NOT NULL,
            hotphrase7 TEXT NOT NULL,
            hotphrase8 TEXT NOT NULL,
            hotphrase9 TEXT NOT NULL,
            hotphrase10 TEXT NOT NULL,
            hotphrase11 TEXT NOT NULL,
            hotphrase12 TEXT NOT NULL,
            hotphrase13 TEXT NOT NULL,
            hotphrase14 TEXT NOT NULL,
            hotphrase15 TEXT NOT NULL,
            hotphrase16 TEXT NOT NULL,
            hotphrase17 TEXT NOT NULL,
            hotphrase18 TEXT NOT NULL,
            hotphrase19 TEXT NOT NULL,
            hotphrase20 TEXT NOT NULL,
            hotphrase21 TEXT NOT NULL,
            hotphrase22 TEXT NOT NULL,
            hotphrase23 TEXT NOT NULL,
            hotphrase24 TEXT NOT NULL,
            hotphrase25 TEXT NOT NULL,
            hotphrase26 TEXT NOT NULL,
            hotphrase27 TEXT NOT NULL,
            hotphrase28 TEXT NOT NULL,
            hotphrase29 TEXT NOT NULL,
            hotphrase30 TEXT NOT NULL,
            hotphrase31 TEXT NOT NULL,
            hotphrase32 TEXT NOT NULL,
            hotphrase33 TEXT NOT NULL,
            hotphrase34 TEXT NOT NULL,
            hotphrase35 TEXT NOT NULL,
            hotphrase36 TEXT NOT NULL,
            hotphrase37 TEXT NOT NULL,
            hotphrase38 TEXT NOT NULL,
            hotphrase39 TEXT NOT NULL,
            hotphrase40 TEXT NOT NULL,
            hotphrase41 TEXT NOT NULL,
            hotphrase42 TEXT NOT NULL,
            hotphrase43 TEXT NOT NULL,
            hotphrase44 TEXT NOT NULL,
            hotphrase45 TEXT NOT NULL,
            hotphrase46 TEXT NOT NULL,
            hotphrase47 TEXT NOT NULL,
            hotphrase48 TEXT NOT NULL,
            hotphrase49 TEXT NOT NULL,
            hotphrase50 TEXT NOT NULL,
            PRIMARY KEY (date, time)
        );

        {}
        '''.format(user_table_command))


    def upload_Query_data(self,query_log):
        '''
        upload all the Query data into the Query table

        Arguments:
        query_log -- A list of the path of the query logs
            example:
            query_log = [".\Deep_Learning\querylog_201903151407.txt",".\Deep_Learning\querylog_201903151408.txt"]

        Return:
        flag -- A bool variable suggests whether the operation is successed or not.
        '''

        file_order = 0
        for log in query_log:
            with open(log,'r', encoding='UTF-8') as fd:
        
                lines = fd.readlines()
                print ('Total line numbers are: %s' %(len(lines)))
                print('This is log {}',format(file_order))

            cur_ = 1
            for line in tqdm(lines):
                
                fields = line.strip().split("\t")
                if len(fields)!= 7:
                    continue
                
                d_t = fields[0].split(" ")
                if len(d_t) == 0:
                    continue
            
                try:
                    phrase = fields[1]
                    uid = int(fields[2])
                    if uid == 0:
                        continue
                except:
                    print(fields)
                    continue
            
                try:
                    behavior_code = fields[3]
                except:
                    print("\n",fields)
                    continue

                if behavior_code == '1' or behavior_code == '2' or behavior_code == '3':
                    behavior = 0 # 0 reprents user searched by him self
                elif behavior_code == '30':
                    behavior = 1 # 1 reprents user clicked the phrase in the box
                elif behavior_code == '31':
                    behavior = 2 # 2 reprents user clicked the phrase on the board but not in the box
                else:
                    continue

                try:
                    util = fields[4]
                except:
                    print("\n",fields)
                    continue

                try:
                    self.cur.execute('''INSERT OR IGNORE INTO Recommender (date, time, phrase, uid, behavior, util) 
                    VALUES ( ?, ?, ?, ?, ?, ?)''', ( d_t[0] , d_t[1], phrase, uid, behavior, util) )
                except:
                    continue
                
                cur_ +=1
                if cur_ % 10000 == 0:
                    self.conn.commit()
            
            file_order += 1

        self.conn.commit()
        #cur.close()
        self.cur.execute('SELECT * FROM Query')
        top = self.cur.fetchone()
        if len(top[0]):
            return True
        else:
            print("There is a problem!")
            return False


    def upload_Recommender_data(self,recommender_log):
        '''
        upload all the Recommender data into the Recommender table

        Arguments:
        recommender_log -- A list of the path of the recommender logs
            example:
            recommender_log = [".\Deep_Learning\\recommender_log.201903191130.txt",".\Deep_Learning\\recommender_log.201903191130.txt"]

        Return:
        flag -- A bool variable suggests whether the operation is successed or not.
        '''

        #print(recommender_log)

        file_order = 0
        for log in recommender_log:
            with open(log,'r', encoding='UTF-8') as fd:
        
                lines = fd.readlines()
                print ('Total line numbers are: %s' %(len(lines)))
                print('This is log {}'.format(file_order))

            cur_ = 1
            for line in tqdm(lines):
            
                fields = line.strip().split("\t")
                if len(fields)!= 12:
                    continue
                
                d_t = fields[0].split(" ")
                if len(d_t) == 0:
                    continue
            
                try:
                    most_hot = fields[3]
                    uid = int(fields[1])
                    if uid == 0:
                        continue
                except:
                    print(fields)
                    continue
            
                try:
                    special_push = fields[4]
                except:
                    print("\n",fields)
                    continue

                """ if behavior_code == '1' or behavior_code == '2' or behavior_code == '3':
                    behavior = 0 # 0 reprents user searched by him self
                elif behavior_code == '30':
                    behavior = 1 # 1 reprents user clicked the phrase in the box
                elif behavior_code == '31':
                    behavior = 2 # 2 reprents user clicked the phrase on the board but not in the box
                else:
                    continue """
                
                temp_recommend = []
                for i in range(5,11):
                    try:
                        temp_recommend.append(fields[i])
                    except:
                        print("\n",fields)
                        continue
                
                try:
                    self.cur.execute('''INSERT OR IGNORE INTO Recommender (date, time, most_hot, special_push,
                                     recommend1, recommend2, recommend3, recommend4, recommend5, recommend6, uid)
                                      VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?)''', \
                                          ( d_t[0] , d_t[1], most_hot, special_push, \
                                              temp_recommend[0], temp_recommend[1], temp_recommend[2], \
                                                temp_recommend[3], temp_recommend[4], temp_recommend[5], uid) )
                except:
                    """ print(( d_t[0] , d_t[1], most_hot, special_push, \
                                              temp_recommend[0], temp_recommend[1], temp_recommend[2], \
                                                temp_recommend[3], temp_recommend[4], temp_recommend[5]))  """
                    continue
                
                cur_ +=1
                if cur_ % 10000 == 0:
                    self.conn.commit()
            
        self.conn.commit()
        #cur.close()
        self.cur.execute('SELECT * FROM Recommender')
        top = self.cur.fetchone()
        if len(top[0]):
            return True
        else:
            print("There is a problem!")
            return False


    def upload_Hot_phrase_data(self,Hot_phrase_log):
        '''
        upload all the Hot_phrase data into the Hot_phrase table

        Arguments:
        Hot_phrase_log -- A list of the path of the Hot_phrase logs
            example:
            Hot_phrase_log = [".\Deep_Learning\hot20190319-1130.txt",".\Deep_Learning\hot20190319-1130.txt"]

        Return:
        flag -- A bool variable suggests whether the operation is successed or not.
        '''

        file_order = 0
        for log in Hot_phrase_log:
            with open(log,'r', encoding='UTF-8') as fd:
        
                lines = fd.readlines()
                print ('Total line numbers are: %s' %(len(lines)))
                print('This is log {}',format(file_order))

            h_list = []
            for line in lines:
                h_list.append(line[0])

            try:
                date0 = re.findall('t(.*)-',log)[0]
                date = date0[:4] + '-' + date0[4:6] + '-' + date0[6:] 
                time0 = re.findall('-(.*).txt',log)[0]
                time = time0[:2] + ":" + time0[2:] + ":" + "00"
                d_t = [date,time]
            except:    
                continue
            
            temp = []
            for line in h_list:
                fields = line.strip().split("\t")
                if len(fields)!= 11:
                    continue
                
                try:
                    phrase = fields[0]
                    temp.append(phrase)
                except:
                    print(fields)
                    continue
                
                """ try:
                    for i in range (1,11):
                        temp.append(fields[i])
                except:
                    print("\n",fields)
                    continue """

                """ if behavior_code == '1' or behavior_code == '2' or behavior_code == '3':
                    behavior = 0 # 0 reprents user searched by him self
                elif behavior_code == '30':
                    behavior = 1 # 1 reprents user clicked the phrase in the box
                elif behavior_code == '31':
                    behavior = 2 # 2 reprents user clicked the phrase on the board but not in the box
                else:
                    continue """
                
                info_tuple = tuple(d_t + temp)
                try:
                    self.cur.execute('''INSERT OR IGNORE INTO Hotphrase (date, time, 
                    hotphrase1, hotphrase2, hotphrase3, hotphrase4, hotphrase5, hotphrase6,
                    hotphrase7, hotphrase8, hotphrase9, hotphrase10, hotphrase11, hotphrase12,
                    hotphrase13, hotphrase14, hotphrase15, hotphrase16, hotphrase17, hotphrase18,
                    hotphrase19, hotphrase20, hotphrase21, hotphrase22, hotphrase23, hotphrase24,
                    hotphrase25, hotphrase26, hotphrase27, hotphrase28, hotphrase29, hotphrase30,
                    hotphrase31, hotphrase32, hotphrase33, hotphrase34, hotphrase35, hotphrase36,
                    hotphrase37, hotphrase38, hotphrase39, hotphrase40, hotphrase41, hotphrase42,
                    hotphrase43, hotphrase44, hotphrase45, hotphrase46, hotphrase47, hotphrase48,
                    hotphrase49, hotphrase50)
                                      VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?,
                                      ?, ?, ?, ?, ?, ?, ?, ?, ? ,?,
                                      ?, ?, ?, ?, ?, ?, ?, ?, ? ,?,
                                      ?, ?, ?, ?, ?, ?, ?, ?, ? ,?,
                                      ?, ?, ?, ?, ?, ?, ?, ?, ? ,? )''', \
                                          info_tuple)
                except:
                    continue
                
        self.conn.commit()
        #cur.close()
        self.cur.execute('SELECT * FROM Recommender')
        top = self.cur.fetchone()
        if len(top[0]):
            return True
        else:
            print("There is a problem!")
            return False



class SQL_out:
    
    def __init__(self,path):
        '''
        construct function.
        
        Path is the path of the sqlite database, string.
        After this we shall use self.cur to connect the sqlite database
        '''
        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()

    def __del__(self):
        '''
        destructor function
        
        We disconnect with the database
        '''
        self.conn.commit()
        self.cur.close()

    def connect(self,path):
        '''
        path is the path of the sqlite database, string.
        after this we shall use self.cur to connect the sqlite database
        '''
        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()
    
    def disconnect(self):
        self.conn.commit()
        self.cur.close()
    

    def select_query_by_up_bnd(self, temp_query_list, up_bnd):
        '''
        filter the query history in the up_bnd mode
        
        Argument:
        up_bnd -- the upper bound of number of query history for each user
        
        Return:
        query_list -- the list of user,query n-elements tuple, e.g. [(uid, q1, q2, ... , qk)]
        '''
        
        query_tuples = []
        
        try:
            #Here we first initialize a dictionary to count how much hisory each user have.
            query_dict =  {}
            for query in temp_query_list:

                if query[0] in query_dict:
                    query_dict[query[0]] += 1
                else:
                    query_dict[query[0]] = 1
                
                if query_dict[query[0]] == up_bnd+1:
                    continue
                
                query_tuples.append(query)
            return query_tuples
        except:
            print('there is somethin wrong with your list or tuples')

    def select_query_equally(self,temp_query_list, spl_per_user):
        
        query_tuples = []
        
        try:
            #Here we first initialize a dictionary to count how much hisory each user have.
            query_dict =  {}
            for query in temp_query_list:

                if query[0] in query_dict:
                    query_dict[query[0]] += 1
                else:
                    query_dict[query[0]] = 1
                
                if query_dict[query[0]] == spl_per_user + 1:
                    continue
                
                query_tuples.append(query)
            
            return_list = []
            for item in query_tuples:
                if query_dict[item[0]] >= spl_per_user:
                    return_list.append(item)

            return return_list
        except:
            print('there is somethin wrong with your list or tuples')
    
    def fetch_history_query(self,time_start, time_end, mode = 'window', up_bnd = 10, spl_per_user = 5, rt_df = False):
        '''
        Fetch the list of all needed uid query history for constructing the training set and dev set
        
        Argument: 
        time_start -- the start date and time of the query history, dtype:tuple('YYYY-MM-DD', 'HH:MM:SS')
        time_end -- the end date and time of the query history, dtype:tuple('YYYY-MM-DD', 'HH:MM:SS')

        mode -- the mode for abstract the query history. In total, there are three modes, which are:
                'window' means fetch all the query history in a certain time window.
                'up_bnd' means fetch the query history of each user but control the number of query within the bound.
                        For example, if the upper bound is 10, we know at most we would fetch 10 queries for a certain user.
                'equl' means fetch equal number of query history from each user. For example, we would fetch latest 10
                        sampels for each user and ignore whether or not he or she is an active user.
                Among all the three mode, we preferentially fetch the most recent samples for each users.

        up_bnd -- used in the 'up_bnd' mode, the upper bound of the number of query history for each user

        spl_per_user -- used in the 'equl' mode, the number of query history for each user

        rt_df -- whether to return a data frame. This may be helpful when one want to do analysis
        
        Return:
        
        query_list -- the list of user,query n-elements tuple, e.g. [(uid, q1, q2, ... , qk)]
        query_df -- the DataFrame containes all the required query history

        '''
        if mode == 'window':
            self.cur.execute('''SELECT uid, phrase, date, time FROM Query WHERE date >= ? AND date <= ? AND time >= ? 
                                AND time <= ? ORDER BY date, time''',  \
                (time_start[0], time_end[0], time_start[1], time_end[1]))
            self.query_list = self.cur.fetchall()
            return self.query_list
        
        if mode == 'up_bnd':
            self.cur.execute('''SELECT uid, phrase, date, time FROM Query WHERE date >= ? AND date <= ? AND time >= ? 
                                AND time <= ? ORDER BY date, time''',  \
                (time_start[0], time_end[0], time_start[1], time_end[1]))
            temp_query_list = self.cur.fetchall()
            ret = self.select_query_by_up_bnd(temp_query_list, up_bnd)
            return ret
        
        if mode == 'constraint':
            self.cur.execute('''SELECT uid, phrase, date, time FROM Query WHERE date >= ? AND date <= ? AND time >= ? 
                                AND time <= ? ORDER BY date, time''',  \
                (time_start[0], time_end[0], time_start[1], time_end[1]))
            temp_query_list = self.cur.fetchall()
            ret = self.select_query_equally(temp_query_list, spl_per_user)
            return ret
        
        else:
            print('chack your mode!')

    def fetch_history_recommended_word(self, uid_tuples, contrast_mode = 'normal', negative_sample = 5):
        if contrast_mode == 'normal':
            pass