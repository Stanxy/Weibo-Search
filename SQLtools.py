import sqlite3
#import Embeddingtools
import datetime
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


    def initialize_db(self):
        '''
        function used to initialize the database 
        '''
        self.cur.executescript('''
        DROP TABLE IF EXISTS Query;
        DROP TABLE IF EXISTS Recommender;
        DROP TABLE IF EXISTS Hotphrase;
        DROP TABLE IF EXISTS Userinterests;

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
            special_push TEXT NOT NULL,
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

        CREATE TABLE Userinterests(
            uid INTEGER PRIMARY kEY,
            interest1 REAL, interest2 REAL, interest3 REAL, interest4 REAL, interest5 REAL,
            interest6 REAL, interest7 REAL, interest8 REAL, interest9 REAL, interest10 REAL,
            interest11 REAL, interest12 REAL, interest13 REAL, interest14 REAL, interest15 REAL,
            interest16 REAL, interest17 REAL, interest18 REAL, interest19 REAL, interest20 REAL,
            interest21 REAL, interest22 REAL, interest23 REAL, interest24 REAL, interest25 REAL            
        );

        ''')

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
                print('This is log {}'.format(file_order))

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
                    self.cur.execute('''INSERT OR IGNORE INTO Query (date, time, phrase, uid, behavior, util) 
                    VALUES ( ?, ?, ?, ?, ?, ?)''', ( d_t[0] , d_t[1], phrase, uid, behavior, util) )
                except:
                    continue
                #############
                #if cur_ == 10:
                #    break
                ############
                cur_ +=1
                if cur_ % 10000 == 0:
                    self.conn.commit()
            
            file_order += 1

        self.conn.commit()
        #cur.close()
        self.cur.execute('SELECT * FROM Query')
        top = self.cur.fetchone()
        try:
            len(top[0])
            return True
        except:
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
            
            file_order += 1

        self.conn.commit()
        #cur.close()
        self.cur.execute('SELECT * FROM Recommender')
        top = self.cur.fetchone()
        if len(top[0]):
            return True
        else:
            print("There is a problem!")
            return False


    def upload_Hotphrase_data(self,Hotphrase_log):
        '''
        upload all the Hot_phrase data into the Hot_phrase table

        Arguments:
        Hotphrase_log -- A list of the path of the Hot_phrase logs
            example:
            Hotphrase_log = [".\Deep_Learning\hot20190319-1130.txt",".\Deep_Learning\hot20190319-1130.txt"]

        Return:
        flag -- A bool variable suggests whether the operation is successed or not.
        '''

        file_order = 0
        for log in Hotphrase_log:
            with open(log,'r', encoding='UTF-8') as fd:
        
                lines = fd.readlines()
                print ('Total line numbers are: %s' %(len(lines)))
                print('This is log {}'.format(file_order))

            h_list = []
            count = 0
            for line in lines:
                fields = line.strip().split("\t")
                if count < 50:
                    h_list.append(fields[0])
                    count += 1
                    #print(h_list)
                    #print(count)
                else:
                    break
            
            try:
                date0 = re.findall('t(.*)-',log)[0]
                date = date0[:4] + '-' + date0[4:6] + '-' + date0[6:] 
                time0 = re.findall('-(.*).txt',log)[0]
                time = time0[:2] + ":" + time0[2:] + ":" + "00"
                d_t = [date,time]
                #print(d_t)
            except:    
                continue

            info_tuple = tuple(d_t + h_list)

            #print(info_tuple)
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
            file_order += 1     
        self.conn.commit()
        #cur.close()
        self.cur.execute('SELECT * FROM Recommender')
        top = self.cur.fetchone()
        if len(top[0]):
            return True
        else:
            print("There is a problem!")
            return False


    def upload_Userinterests_data(self,Userinterests_log):
        '''
        upload all the Userinterests data into the Userinterests table

        Arguments:
        Userinterests_log -- A list of the path of the Userinterests logs
            example:
            Userinterests_log = [".\Deep_Learning\cuid_feature.txt"]

        Return:
        flag -- A bool variable suggests whether the operation is successed or not.
        '''

        file_order = 0
        for log in Userinterests_log:
            with open(log,'r', encoding='UTF-8') as fd:
        
                lines = fd.readlines()
                print ('Total line numbers are: %s' %(len(lines)))
                print('This is log {}'.format(file_order))

            cur_ = 1
            for line in tqdm(lines):
                
                fields = line.strip().split("\t")
                #print(fields)
                if len(fields)!= 2:
                    continue
                
                #try:
                uid = []
                uid.append(int(fields[0]))
                interests_list = fields[1].split('|')
                for i in range(len(interests_list)):
                    interests_list[i] = float(interests_list[i])

                temp_tuple = tuple(uid + interests_list)
                #print(temp_tuple)
                #if uid[0] == 0:
                #    continue
                #except:
                #    print(fields)
                #    continue
                    
                try:
                    self.cur.execute('''INSERT OR IGNORE INTO Userinterests (
                    uid, interest1 , interest2 , interest3 , interest4 , interest5 ,
                    interest6 , interest7 , interest8 , interest9 , interest10 ,
                    interest11 , interest12 , interest13 , interest14 , interest15 ,
                    interest16 , interest17 , interest18 , interest19 , interest20 ,
                    interest21 , interest22 , interest23 , interest24 , interest25 ) 
                    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', \
                        temp_tuple )
                except:
                    continue
                
                cur_ +=1
                if cur_ % 10000 == 0:
                    self.conn.commit()
            
            file_order += 1

        self.conn.commit()
        #cur.close()
        self.cur.execute('SELECT * FROM Userinterests')
        top = self.cur.fetchone()
        #print(top)
        if len(top):
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
        query_list -- the list of tuples [(date, time, phrase, uid, behavior)]
        '''
        
        query_tuples = []
        
        try:
            #Here we first initialize a dictionary to count how much hisory each user have.
            query_dict =  {}
            for query in tqdm(temp_query_list):

                if query[3] in query_dict:
                    query_dict[query[3]] += 1
                else:
                    query_dict[query[3]] = 1
                
                if query_dict[query[3]] == up_bnd+1:
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
            for query in tqdm(temp_query_list):
                #print(query)
                if query[3] in query_dict:
                    query_dict[query[3]] += 1
                else:
                    query_dict[query[3]] = 1
                
                if query_dict[query[3]] == spl_per_user + 1:
                    continue
                
                query_tuples.append(query)
            
            return_list = []
            for item in query_tuples:
                #print(item)
                if query_dict[item[3]] >= spl_per_user:
                    return_list.append(item)

            return return_list
        except:
            print('there is somethin wrong with your list or tuples')
    
    def fetch_sample_query(self,time_start, time_end, mode = 'window', up_bnd = 10, spl_per_user = 5, rt_df = False):
        '''
        Fetch the list of all needed uid query sample for constructing the training set and dev set
        
        Argument: 
        time_start -- the start date and time of the query samples, dtype:tuple('YYYY-MM-DD', 'HH:MM:SS')
        time_end -- the end date and time of the query samples, dtype:tuple('YYYY-MM-DD', 'HH:MM:SS')

        mode -- the mode for extract the query samples. In total, there are three modes, which are:
                'window' means fetch all the query samples in a certain time window.
                'up_bnd' means fetch the query samples of each user but control the number of query within the bound.
                        For example, if the upper bound is 10, we know at most we would fetch 10 queries for a certain user.
                'equl' means fetch equal number of query samples from each user. For example, we would fetch latest 10
                        sampels for each user and ignore whether or not he or she is an active user.
                Among all the three mode, we preferentially fetch the most recent samples for each users.

        up_bnd -- used in the 'up_bnd' mode, the upper bound of the number of query samples for each user

        spl_per_user -- used in the 'equl' mode, the number of query samples for each user

        rt_df -- whether to return a data frame. This may be helpful when one want to do analysis
        
        Return:
        
        new_user_list -- the list of tuples [(date, time, phrase, uid, behavior)]
        '''
        if mode == 'window':
            self.cur.execute('''SELECT date, time, phrase, uid, behavior FROM Query WHERE date >= ? AND date <= ? AND time >= ?
                                AND time <= ? ORDER BY date, time DESC''',  \
                (time_start[0], time_end[0], time_start[1], time_end[1]))
            self.query_list = self.cur.fetchall()
            return self.query_list
        
        if mode == 'up_bnd':
            self.cur.execute('''SELECT date, time, phrase, uid, behavior FROM Query WHERE date >= ? AND date <= ? AND time >= ?
                                AND time <= ? ORDER BY date, time DESC''',  \
                (time_start[0], time_end[0], time_start[1], time_end[1]))
            temp_query_list = self.cur.fetchall()
            ret = self.select_query_by_up_bnd(temp_query_list, up_bnd)
            return ret
        
        if mode == 'constraint':
            self.cur.execute('''SELECT date, time, phrase, uid, behavior FROM Query WHERE date >= ? AND date <= ? AND time >= ?
                                AND time <= ? ORDER BY date, time DESC''',  \
                (time_start[0], time_end[0], time_start[1], time_end[1]))
            temp_query_list = self.cur.fetchall()
            ret = self.select_query_equally(temp_query_list, spl_per_user)
            return ret
        
        else:
            print('chack your mode!')

    def fetch_sample_recommended(self, user_tuples, contrast_mode = 'normal', negative_samples = 5):
        '''
        Fetch the list of all needed uid recommend samples for constructing the training set
        
        Argument: 
        user_tuples -- user tuples that used to control the uid and date and time [(date, time, phrase, uid, behavior)]
        
        contrast_mode -- the mode for extract the recommend samples. In total, there are two modes, which are:
                'normal' means fetch top n recommended phrases in the recommended box.
                'special' means fetch the special_push word (the phrase which ranks the second on the recommended box).

        negative_samples -- used in teh 'normal' mode, the number of negative examples you want to extract from the box
        
        Return:
        
        new_user_list -- the list of tuples [(uid, date, time, phrase, recommend1, recommend2, ..., recommend_{negative_samples} )]
                        if the 'special' mode is on, then the return will be [(uid, date, time, phrase, special)]
        '''
        if negative_samples >= 6:
            print('Too much samples required, the number of negative_samples should below 5')
            return None
        case_num = 0
        if contrast_mode == 'normal':
            new_user_list = []
            case_not_found = []
            for case in tqdm(user_tuples):
                # Here negative sample is used to control the number of negative_samples.
                self.cur.execute(''' SELECT recommend1, recommend2, recommend3, recommend4, recommend5, recommend6
                                FROM Recommender WHERE uid = ? AND date <= ? AND time < ? ORDER BY date, time DESC''', (case[3],case[0],case[1]))
                recommended_phrases = []
                count = 0
                #print(case[3])
                try:
                    recommends = list(self.cur.fetchall())
                    for phrase in recommends[0]:
                        #print(phrase)
                        if phrase != case[2] and count <= negative_samples:
                            count += 1
                            recommended_phrases.append(phrase)
                            id = []
                            id.append(case[3])
                    new_case = tuple(id + list(case[:3]) + list(recommended_phrases)) 
                    new_user_list.append(new_case)
                    case_num += 1
                except:
                    new_user_list.append(-1)
                    case_not_found.append(case_num)
                    case_num += 1
                    continue
                    
            return new_user_list, case_not_found
        
        if contrast_mode  == 'special':
            new_user_list = []
            case_not_found = []
            for case in tqdm(user_tuples):
                self.cur.execute('''SELECT special_push FROM Recommender WHERE uid = ? AND date <=? AND time < ? ORDER BY date, time DESC''', \
                                (case[3],case[0],case[1]))
                id = []
                id.append(case[3])
                try:
                    special_push = list(self.cur.fetchall())[0][0]
                    #print(case[3],case[0],case[1])
                    len(special_push)
                    #print(self.cur.fetchall()[0])
                    new_case = tuple(id + list(case[:3]) + [special_push])
                    new_user_list.append(new_case)
                    case_num += 1
                except:
                    new_user_list.append(-1)
                    case_not_found.append(case_num)
                    case_num += 1
                    continue
                
            return new_user_list, case_not_found
    
    def fetch_sample_hotphrase(self, user_tuples):
        '''
        Fetch the hot phrase samples in the hot list 
        
        Arguments:
        user_tuples -- user tuples that used to control the uid and date and time [(date, time, phrase, uid, behavior)]

        Return:
        user_hotphrase_tuple_list -- the list of tuples [(uid, date, time, phrase, hotphrase1, horphrase2, ..., hotphrase50)]
        '''
        user_hotphrase_tuple_list = []
        case_not_found = []
        case_num = 0
        for case in tqdm(user_tuples):
            date_upper_bound = case[0].split('-')
            time_upper_bound = case[1].split(':')
            d_t_obj = datetime.datetime(int(date_upper_bound[0]),int(date_upper_bound[1]),int(date_upper_bound[2]), \
                                int(time_upper_bound[0]), int(time_upper_bound[1]), int(time_upper_bound[2]))
            delta_plus = datetime.timedelta(minutes = 1)
            delta_minus = datetime.timedelta(minutes = -1)
            d_t_obj_upper = d_t_obj + delta_plus
            d_t_obj_lower = d_t_obj + delta_minus
            window_upper_bound = d_t_obj_upper.strftime('%Y-%m-%d %H:%M:%S')[11:]
            window_lower_bound = d_t_obj_lower.strftime('%Y-%m-%d %H:%M:%S')[11:]
            self.cur.execute('''SELECT 
                    hotphrase1, hotphrase2, hotphrase3, hotphrase4, hotphrase5, hotphrase6,
                    hotphrase7, hotphrase8, hotphrase9, hotphrase10, hotphrase11, hotphrase12,
                    hotphrase13, hotphrase14, hotphrase15, hotphrase16, hotphrase17, hotphrase18,
                    hotphrase19, hotphrase20, hotphrase21, hotphrase22, hotphrase23, hotphrase24,
                    hotphrase25, hotphrase26, hotphrase27, hotphrase28, hotphrase29, hotphrase30,
                    hotphrase31, hotphrase32, hotphrase33, hotphrase34, hotphrase35, hotphrase36,
                    hotphrase37, hotphrase38, hotphrase39, hotphrase40, hotphrase41, hotphrase42,
                    hotphrase43, hotphrase44, hotphrase45, hotphrase46, hotphrase47, hotphrase48,
                    hotphrase49, hotphrase50 FROM Hotphrase WHERE time >= ? AND time <= ? AND date = ? ORDER BY date, time DESC''', (window_lower_bound, window_upper_bound, case[0]))
            id = []
            id.append(case[3])
            try:
                user_hotphrase_tuple_list.append(tuple(id + list(case[:3]) + list(self.cur.fetchone())))
                case_num += 1
            except:
                user_hotphrase_tuple_list.append(-1)
                case_not_found.append(case_num)
                case_num += 1
                continue
        return user_hotphrase_tuple_list, case_not_found

    def fetch_query_history(self, user_tuples, mode = 'window', times = 10, window = 7):
        '''
        Exstract the user query history for constract the input. Since this process requires SQL interaction,
        we simply put it in the SQL_out class.

        Arguments:
        user_tuples -- the scope of users you want [(date, time, phrase, uid, behavior)]

        mode -- the mode for extracting the query history. In total, there are three modes, which are:
                'window' means fetch all the query history in a certain time window.
                'times' means fetch the query history of each user for how many times
                Among all the two mode, we preferentially fetch the most recent samples for each users.

        times -- the number of history queries for each user
        window -- the time window of history queries for each user

        Return:
        user_query_history -- a list of uid and query tuples [(uid, q1, q2, ..., qn)]
        '''
        user_query_history = []
        case_not_found = []
        case_num = 0
        for case in tqdm(user_tuples):
            uid = case[3]
            if mode == 'times':
                self.cur.execute('''SELECT phrase FROM Query WHERE uid = ? AND date <= ? 
                                AND time < ? ORDER BY date, time LIMIT ? ORDER BY date, time DESC''', \
                                (uid, case[0], case[1], times))
                temp_list = []
                phrases = list(self.cur.fetchall())
                for phrase in phrases:
                    temp_list.append(phrase[0])
                    #print(phrase[0])
                if len(temp_list) != 0:
                    user_query_history.append(tuple([case[3]] + temp_list))
                    case_num += 1
                else:
                    user_query_history.append(-1)
                    case_not_found.append(case_num)
                    case_num += 1
                #return user_query_history, case_not_found

            if mode == 'window':
                date_upper_bound = case[0].split('-')
                time_upper_bound = case[1].split(':')
                d_t_obj_upper = datetime.datetime(int(date_upper_bound[0]),int(date_upper_bound[1]),int(date_upper_bound[2]), \
                                    int(time_upper_bound[0]), int(time_upper_bound[1]), int(time_upper_bound[2]))
                delta = datetime.timedelta(days = -window)
                d_t_obj_lower = d_t_obj_upper + delta
                window_lower_bound = d_t_obj_lower.strftime('%Y-%m-%d %H:%M:%S')[:10]

                #print(uid, case[0], case[1],window_lower_bound)
                self.cur.execute('''SELECT phrase FROM Query WHERE uid = ? AND date <= ? 
                                AND time < ? AND date >= ? ORDER BY date, time DESC''', \
                                (uid, case[0], case[1], window_lower_bound))
                temp_list = []
                phrases = list(self.cur.fetchall())
                for phrase in phrases:
                    temp_list.append(phrase[0])
                if len(temp_list) != 0:
                    user_query_history.append(tuple([case[3]] + temp_list))
                    case_num += 1
                else:
                    user_query_history.append(-1)
                    case_not_found.append(case_num)
                    case_num += 1
                    continue
        return user_query_history, case_not_found

    def fetch_user_interest(self, user_tuples):
        '''
        Exstract the user interest for constract the input. Since this process requires SQL interaction,
        we simply put it in the SQL_out class.

        Arguments:
        user_tuples -- the scope of users you want [(date, time, phrase, uid, behavior)]

        Return:
        user_interest -- a list of uid and interest tuples [(uid, intest1, interest2, ..., interest 25)]
        '''
        user_interest = []
        case_not_found = []
        case_num = 0
        for case in tqdm(user_tuples):
            uid = case[3]
            self.cur.execute('''SELECT uid, interest1 , interest2 , interest3 , interest4 , interest5 ,
            interest6 , interest7 , interest8 , interest9 , interest10 ,
            interest11 , interest12 , interest13 , interest14 , interest15 ,
            interest16 , interest17 , interest18 , interest19 , interest20 ,
            interest21 , interest22 , interest23 , interest24 , interest25   
            FROM Userinterests WHERE uid = ?''', (uid,))
            try:
                temp_tuple = self.cur.fetchone()
                len(temp_tuple)
                user_interest.append(temp_tuple)
                case_num += 1
            except:
                user_interest.append(-1)
                case_not_found.append(case_num)
                case_num += 1
                continue
        return user_interest, case_not_found