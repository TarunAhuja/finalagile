import MySQLdb;
import re
import nltk
from HTMLParser import HTMLParser

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

class EnntityExtractor:

    db = None

    def __init__(self):
        self.db = MySQLdb.connect(host="127.0.0.1.",user="root",passwd="123456",db="xataka1",port=3307)
        self.cursor = self.db.cursor()

    def createTables(self):
        self.cursor.execute("CREATE TABLE temp_satin(entity_name varchar(200),post_id int)") 
        self.cursor.execute("CREATE TABLE post2entity_satin(post_id int,entity_id int)")
        self.cursor.execute("CREATE TABLE entities_satin(entity_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, entity_name VARCHAR(200))")

    def entity_extractor(self, data):
        #rename z
        z= data.split()
        text = nltk.word_tokenize(data)
        
        tagged = nltk.pos_tag(text)                                       ##nltk.pos_tag() assigns tag to each token
        namedentities = nltk.chunk.ne_chunk(tagged,binary=True)
        entities = re.findall(r'NE\s(.*?)/',str(namedentities))
        entities = list(set(entities))
        entity = []
        for i in entities:
            for j in range(len(z)):
                if i == z[j]:
                    try:
                        s=re.findall(r'\w',z[j+1])
                        s1=re.findall(r'\w',z[j+2])
                        s2=re.findall(r'\w',z[j+3])
                        m=len(s)
                        m1=len(s1)
                        m2=len(s2)
                        p=len(z[j+1])
                        p1=len(z[j+2])
                        p2=len(z[j+3])
                        if m!=p:
                            continue
                        else:
                            entity.append(i+' '+' '+z[j+1])
                        if m1!=p1:
                            continue
                        else:
                            entity.append(i+' '+z[j+1]+' '+z[j+2])
                        if m2!=p2:
                            continue
                        else:
                            entity.append(i+' '+z[j+1]+' '+z[j+2]+' '+z[j+3])
                
                    except:
                        continue
        
        entity=list(set(entity))
        entities.extend(entity)
        return entities

    def execute(self):
        done = True
        offset = 0
        while done:
            placeholder = []
            alltags = []
            sql = "SELECT id, post_title, post_content FROM wp_posts where post_type = 'normal' and post_status = 'publish' LIMIT 10 OFFSET %d" %(offset)
            self.cursor.execute(sql);
            data = self.cursor.fetchall()
            if data:                                                        ##data contains all post_id's,post_title's and post_content's
                for row in data:
                    s=strip_tags(row[2])
                    tags = self.entity_extractor(s)                         ##row[2] contains the post_content which is processed by entity_extractor() to extract entities from
                    placeholder.extend("(%s, %s)" for i in range(len(tags)))
                    print placeholder
                    for tag in tags:                                        ##list of entities is returned from entity_extractor() and saved in tags
                        alltags.append(tag)                                 ##each tag and post_id is stored in alltags list.
                        alltags.append(row[0])
                self.insertEntitites(placeholder, alltags)
                done = False
            else:
                done = False
            offset += 100;

    def insertEntitites(self, placeholder, entities):
        sql1 = "INSERT INTO temp_satin VALUES " + ",".join(placeholder)
        self.cursor.execute(sql1, entities);                                           
        self.cursor.execute("INSERT INTO entities_satin (entity_name) SELECT DISTINCT entity_name FROM temp_satin");
        self.cursor.execute("INSERT INTO post2entity_satin(post_id,entity_id) SELECT t.post_id,e.entity_id from temp_satin t,entities_satin e where t.entity_name = e.entity_name")
        self.db.commit()
        self.cursor.close()

ee = EnntityExtractor()
ee.createTables()
ee.execute()