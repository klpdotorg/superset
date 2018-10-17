from os import sys
import psycopg2
import datetime
import os
import inspect
import csv

if len(sys.argv) != 5:
    print("Please give database name, database user name, database password and survey id as arguments. USAGE: python import_supsersetdata.py dbname $PGUSER $PGPASSWORD 2")
    sys.exit()


database = sys.argv[1]
user = sys.argv[2]
passwd = sys.argv[3]
survey_id = sys.argv[4]

connectionstring = "dbname=%s user=%s password=%s" % (database,user,passwd)
conn = psycopg2.connect(connectionstring)
cursor = conn.cursor()

sqlselect = "select distinct state,district,block,cluster,institution_name,institution_id,gp,qgid,class,qname,qid,sequence,sum(yescount),sum(numcount),village,gender from (select distinct b0.name as state, b1.name as district, b2.name as block, b3.name as cluster, s.name as institution_name, s.id as institution_id, eb.const_ward_name as gp, qg.id as qgid, qg.name as class, q.question_text as qname, q.id as qid, qgq.sequence as sequence, sum(case ans.answer when 'Yes' then 1 when '1' then 1 when 'No' then 0 when '0' then 0 else 0 end) as yescount, count(ans.answer) as numcount, case when s.village is null then '' else s.village end as village, case when lower(ans1.answer) like 'femal%' then 'Female' when lower(ans1.answer) like  'mal%' then 'Male' end gender from assessments_answergroup_institution ag inner join assessments_answerinstitution ans1 on (ag.id=ans1.answergroup_id and ans1.question_id=291), boundary_boundary b0, boundary_boundary b1, boundary_boundary b2, boundary_boundary b3, schools_institution s, boundary_electionboundary eb, assessments_answerinstitution ans, assessments_questiongroup qg, assessments_question q, assessments_questiongroup_questions qgq where ag.id = ans.answergroup_id and ag.questiongroup_id = qg.id and ans.question_id = q.id and ag.institution_id = s.id and s.admin0_id = b0.id and s.admin1_id = b1.id and s.admin2_id = b2.id and s.admin3_id = b3.id and s.gp_id = eb.id and qg.survey_id = 2 and q.question_text not in ('Gender', 'Class visited') and qgq.questiongroup_id = qg.id and qgq.question_id = q.id and qg.survey_id="+survey_id+" group by b0.name, b1.name, b2.name, b3.name, s.village, s.name, s.id, eb.const_ward_name, qg.id, qg.name, ans.answer, q.question_text, qgq.sequence, q.id,ans1.answer order by sequence)data group by state,district,block,cluster,institution_name,institution_id,gp,qgid,class,qname,qid,sequence,village,gender"

data = {}
cursor.execute(sqlselect,)
qgids = {}
for row in cursor.fetchall():
    state = row[0]
    district = row[1]
    block = row[2]
    cluster = row[3]
    institution_name = row[4]
    institution_id = row[5]
    gp = row[6]
    qgid = str(row[7])
    qg_name = row[8]
    qname = row[9]
    qid = row[10]
    sequence = str(row[11])
    yescount = row[12]
    numcount = row[13]
    village = row[14].replace("'","''")
    gender = row[15]
    if qgid in qgids:
        if sequence not in qgids[qgid]["sequences"]:
            qgids[qgid]["sqlcreate"] = qgids[qgid]["sqlcreate"]+", qname"+sequence+" VARCHAR (50), qid"+sequence+" integer,  yescount"+sequence+" integer,  numcount"+sequence+" integer"
            qgids[qgid]["sequences"][sequence] = 1
    else:
        qgids[qgid] = {"sqlcreate":"","sequences":{}}
        qgids[qgid]["sqlcreate"] = "create table superset_"+qgid+" (state VARCHAR (50), district VARCHAR (50), block VARCHAR (50), cluster VARCHAR (50), village VARCHAR (100), institution_name VARCHAR (100), institution_id integer, gp VARCHAR (100), class VARCHAR (50), gender VARCHAR (50)" 
        qgids[qgid]["sqlcreate"] = qgids[qgid]["sqlcreate"]+", qname"+sequence+" VARCHAR (50), qid"+sequence+" integer, yescount"+sequence+" integer,  numcount"+sequence+" integer"
        qgids[qgid]["sequences"][sequence] = 1
        
    if qgid in data:
        if institution_id in data[qgid]:
            if qg_name in data[qgid][institution_id]["qgs"]:
                if gender in data[qgid][institution_id]["qgs"][qg_name]:
                    if sequence in data[qgid][institution_id]["qgs"][qg_name][gender]["questions"]:
                        data[qgid][institution_id]["qgs"][qg_name][gender]["questions"][sequence]["yescount"] += yescount
                        data[qgid][institution_id]["qgs"][qg_name][gender]["questions"][sequence]["numcount"] += numcount 
                        data[qgid][institution_id]["qgs"][qg_name][gender]["total_numcount"] += numcount 
                        data[qgid][institution_id]["qgs"][qg_name][gender]["total_correctcount"] += yescount 
                    else:
                        data[qgid][institution_id]["qgs"][qg_name][gender]["questions"][sequence] = {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}
                        data[qgid][institution_id]["qgs"][qg_name][gender]["total_numcount"] += numcount 
                        data[qgid][institution_id]["qgs"][qg_name][gender]["total_correctcount"] += yescount 
                else:
                    data[qgid][institution_id]["qgs"][qg_name][gender] = {"questions": {},"total_numcount":0, "total_correctcount":0} 
                    data[qgid][institution_id]["qgs"][qg_name][gender]["total_numcount"] += numcount 
                    data[qgid][institution_id]["qgs"][qg_name][gender]["total_correctcount"] += yescount 
                    data[qgid][institution_id]["qgs"][qg_name][gender]["questions"][sequence] = {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}
            else:
                data[qgid][institution_id]["qgs"][qg_name] = {}
                data[qgid][institution_id]["qgs"][qg_name][gender] = {"questions": {},"total_numcount":0, "total_correctcount":0} 
                data[qgid][institution_id]["qgs"][qg_name][gender]["total_numcount"] += numcount 
                data[qgid][institution_id]["qgs"][qg_name][gender]["total_correctcount"] += yescount 
                data[qgid][institution_id]["qgs"][qg_name][gender]["questions"][sequence] = {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}
        else:
            data[qgid][institution_id] = {"state":state, "district": district, "block": block, "cluster":cluster, "village": village, "institution_name":institution_name, "institution_id":institution_id, "gp": gp, "qgs" :{}}
            data[qgid][institution_id]["qgs"][qg_name] = {gender:{"questions": {sequence: {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}},"total_numcount": 0, "total_correctcount": 0}}
            data[qgid][institution_id]["qgs"][qg_name][gender]["total_numcount"] += numcount 
            data[qgid][institution_id]["qgs"][qg_name][gender]["total_correctcount"] += yescount 
    else:
        data[qgid]={}
        data[qgid][institution_id] = {"state":state, "district": district, "block": block, "cluster":cluster, "village": village, "institution_name":institution_name, "institution_id":institution_id, "gp": gp, "qgs" :{}}
        data[qgid][institution_id]["qgs"][qg_name] = {gender:{"questions": {sequence: {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}}, "total_numcount": 0, "total_correctcount": 0}}
        data[qgid][institution_id]["qgs"][qg_name][gender]["total_numcount"] += numcount 
        data[qgid][institution_id]["qgs"][qg_name][gender]["total_correctcount"] += yescount 
       


for qgid in qgids:
     sqlcreate = qgids[qgid]["sqlcreate"]
     sqlcreate = sqlcreate+", total_correctcount integer, total_numcount integer);"
     cursor.execute(sqlcreate)
     conn.commit()

print("created table")
print("starting data import")


for qgid in data:
    for institution_id in data[qgid]:
        init_sqlinsert = "insert into superset_"+qgid+" values("
        init_sqlinsert = init_sqlinsert +"'"+data[qgid][institution_id]["state"]+"','"+data[qgid][institution_id]["district"]+"','"+data[qgid][institution_id]["block"]+"','"+data[qgid][institution_id]["cluster"]+"','"+data[qgid][institution_id]["village"]+"','"+data[qgid][institution_id]["institution_name"]+"',"+str(institution_id)+",'"+data[qgid][institution_id]["gp"]+"'"
        for qg in data[qgid][institution_id]["qgs"]:
            for gender in data[qgid][institution_id]["qgs"][qg]:
                sqlinsert = init_sqlinsert+",'"+qg+"','"+gender+"'"
                for sequence in data[qgid][institution_id]["qgs"][qg][gender]["questions"]:
                    sqlinsert = sqlinsert+",'"+data[qgid][institution_id]["qgs"][qg][gender]["questions"][sequence]["qname"]+"',"+str(data[qgid][institution_id]["qgs"][qg][gender]["questions"][sequence]["qid"])+","+str(data[qgid][institution_id]["qgs"][qg][gender]["questions"][sequence]["yescount"])+","+str(data[qgid][institution_id]["qgs"][qg][gender]["questions"][sequence]["numcount"])
                sqlinsert = sqlinsert+","+str(data[qgid][institution_id]["qgs"][qg][gender]["total_correctcount"])+","+str(data[qgid][institution_id]["qgs"][qg][gender]["total_numcount"])+");"
            cursor.execute(sqlinsert)
    
conn.commit()
