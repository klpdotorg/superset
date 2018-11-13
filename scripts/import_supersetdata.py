from os import sys
import psycopg2
import datetime
import os
import inspect
import csv

if len(sys.argv) != 6:
    print("Please give host, database name, database user name, database password and survey id as arguments. USAGE: python import_supsersetdata.py host dbname $PGUSER $PGPASSWORD 2")
    sys.exit()


host = sys.argv[1]
database = sys.argv[2]
user = sys.argv[3]
passwd = sys.argv[4]
survey_id = sys.argv[5]

connectionstring = "dbname=%s user=%s password=%s" % (database,user,passwd)
conn = psycopg2.connect(connectionstring)
cursor = conn.cursor()

def get_academicyear(yearmonth):
    year = yearmonth[:4]
    month = yearmonth[-2:]
    if int(month) >=6:
        academicyear = year+"-"+str(int(year)+1)
    else:
        academicyear = str(int(year)-1)+"-"+year
    return academicyear


table_name = "superset_"+survey_id
cursor.execute('DROP TABLE IF EXISTS "'+table_name+'"')

sqlcreate = "create table "+table_name+" (state VARCHAR (50), district VARCHAR (50), block VARCHAR (50), cluster VARCHAR (50), village VARCHAR (100), institution_name VARCHAR (100), institution_id integer, gp VARCHAR (100), class VARCHAR (50), yearmonth integer, academic_year VARCHAR (50), gender VARCHAR (50), qgid integer"
for num in range(1,21):
    sqlcreate = sqlcreate+", qname"+str(num)+" VARCHAR(100), qid"+str(num)+" integer, yescount"+str(num)+" integer, numcount"+str(num)+" integer"
sqlcreate = sqlcreate+", total_correctcount integer, total_numcount integer, numstudents integer);"
cursor.execute(sqlcreate)
conn.commit()

print("created table")

sqlselect = "select distinct state,district,block,cluster,institution_name,institution_id,gp,qgid,class,qname,qid,sequence,sum(yescount),sum(numcount),village,gender,yearmonth,count(distinct agid) as numstudents from (select distinct b0.name as state, b1.name as district, b2.name as block, b3.name as cluster, s.name as institution_name, s.id as institution_id, eb.const_ward_name as gp, qg.id as qgid, qg.name as class, q.question_text as qname, q.id as qid, qgq.sequence as sequence, sum(case ans.answer when 'Yes' then 1 when '1' then 1 when 'No' then 0 when '0' then 0 else 0 end) as yescount, count(ans.answer) as numcount, case when s.village is null then '' else s.village end as village, case when lower(ans1.answer) like 'femal%' then 'Female' when lower(ans1.answer) like  'mal%' then 'Male' else 'Male' end gender,to_char(ag.date_of_visit,'YYYYMM')::int as yearmonth,ag.id as agid from assessments_answergroup_institution ag inner join assessments_answerinstitution ans1 on (ag.id=ans1.answergroup_id and ans1.question_id=291), boundary_boundary b0, boundary_boundary b1, boundary_boundary b2, boundary_boundary b3, schools_institution s, boundary_electionboundary eb, assessments_answerinstitution ans, assessments_questiongroup qg, assessments_question q, assessments_questiongroup_questions qgq where ag.id = ans.answergroup_id and ag.questiongroup_id = qg.id and ans.question_id = q.id and ag.institution_id = s.id and s.admin0_id = b0.id and s.admin1_id = b1.id and s.admin2_id = b2.id and s.admin3_id = b3.id and s.gp_id = eb.id and qg.survey_id = 2 and q.question_text not in ('Gender', 'Class visited') and qgq.questiongroup_id = qg.id and qgq.question_id = q.id group by b0.name, b1.name, b2.name, b3.name, s.village, s.name, s.id, eb.const_ward_name, qg.id, qg.name, ans.answer, q.question_text, qgq.sequence, q.id,ans1.answer , yearmonth,ag.id order by sequence)data group by state,district,block,cluster,institution_name,institution_id,gp,qgid,class,qname,qid,sequence,village,gender, yearmonth"

data = {}
cursor.execute(sqlselect,)


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
    yearmonth = row[16]
    numstudents = row[17]
        
    if qgid in data:
        if institution_id in data[qgid]["institutions"]:
            if yearmonth in data[qgid]["institutions"][institution_id]["yearmonths"]:
                if gender in data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth]:
                    if sequence in data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["questions"]:
                        data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["questions"][sequence]["yescount"] += yescount
                        data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["questions"][sequence]["numcount"] += numcount 
                        data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["total_numcount"] += numcount 
                        data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["total_correctcount"] += yescount 
                    else:
                        data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["questions"][sequence] = {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}
                        data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["total_numcount"] += numcount 
                        data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["total_correctcount"] += yescount 
                else:
                    data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender] = {"questions": {},"total_numcount":numcount, "total_correctcount":yescount, "numstudents": numstudents} 
                    data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["questions"][sequence] = {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}
            else:
                data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth] = {}
                data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender] = {"questions": {},"total_numcount":numcount, "total_correctcount":yescount, "numstudents": numstudents} 
                data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth][gender]["questions"][sequence] = {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}
        else:
            data[qgid]["institutions"][institution_id] = {"state":state, "district": district, "block": block, "cluster":cluster, "village": village, "institution_name":institution_name, "institution_id":institution_id, "gp": gp, "yearmonths" :{}}
            data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth] = {gender:{"questions": {sequence: {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}},"total_numcount": numcount, "total_correctcount": yescount, "numstudents": numstudents}}
    else:
        data[qgid]={"qgname":qg_name,"institutions":{}}
        data[qgid]["institutions"][institution_id] = {"state":state, "district": district, "block": block, "cluster":cluster, "village": village, "institution_name":institution_name, "institution_id":institution_id, "gp": gp, "yearmonths" :{}}
        data[qgid]["institutions"][institution_id]["yearmonths"][yearmonth] = {gender:{"questions": {sequence: {"qid":qid, "qname":qname, "yescount":yescount, "numcount":numcount}}, "total_numcount": numcount, "total_correctcount": yescount, "numstudents": numstudents}}
       

print("starting data import")


for qgid in data:
    for institution_id in data[qgid]["institutions"]:
        init_sqlinsert = "insert into superset_"+survey_id+" values("
        institution_info = data[qgid]["institutions"][institution_id]
        init_sqlinsert = init_sqlinsert +"'"+institution_info["state"]+"','"+institution_info["district"]+"','"+institution_info["block"]+"','"+institution_info["cluster"]+"','"+institution_info["village"]+"','"+institution_info["institution_name"]+"',"+str(institution_id)+",'"+institution_info["gp"]+"','"+data[qgid]['qgname']+"'"
        for yearmonth in institution_info["yearmonths"]:
            yearmonth_info = institution_info["yearmonths"][yearmonth]
            academicyear = get_academicyear(str(yearmonth))
            for gender in yearmonth_info:
                sqlinsert = init_sqlinsert+","+str(yearmonth)+",'"+str(academicyear)+"','"+gender+"',"+qgid
                for sequence in yearmonth_info[gender]["questions"]:
                    question_info = yearmonth_info[gender]["questions"][sequence]
                    sqlinsert = sqlinsert+",'"+question_info["qname"]+"',"+str(question_info["qid"])+","+str(question_info["yescount"])+","+str(question_info["numcount"])
                sqlinsert = sqlinsert+","+str(yearmonth_info[gender]["total_correctcount"])+","+str(yearmonth_info[gender]["total_numcount"])+","+str(yearmonth_info[gender]["numstudents"])+");"
                cursor.execute(sqlinsert)
    
conn.commit()
