import cx_Oracle
import pandas as pd
import json
from sklearn.neighbors import NearestNeighbors
from flask import Flask, request, jsonify
from flask_cors import CORS
# from flask import Flask

app = Flask(__name__)
CORS(app)


@app.route("/", methods=["GET"])
def curating_for_scrap():
    conn = cx_Oracle.connect('HR/a1234@localhost:1521/xe')
    # print(conn.version)
    # SQL 쿼리 실행
    cursor = conn.cursor()
    cursor.execute('select user_keynum, policy_keynum from tb_scrap')
    # for record in cursor:
    #     print(record)
    k = request.args.get('k')
    user_keynum = request.args.get('user_keynum')
    k = int(k)
    user_keynum = int(user_keynum)
    df = pd.DataFrame(cursor.fetchall(), columns =['user_keynum', 'policy_keynum'])
    df['value'] = 1
    # print('df',df)
    df_pivot= pd.pivot_table(df, values=['value'], index=['user_keynum'], columns=['policy_keynum'], fill_value=0)
    # print('df_pivot',df_pivot)
    
    re_model = NearestNeighbors(n_neighbors=k+1, algorithm='brute', metric='cosine').fit(df_pivot)
    dist, ind = re_model.kneighbors(df_pivot.loc[user_keynum].values.reshape(1, -1), n_neighbors=k+1 )
    similar_users = df_pivot.iloc[ind.flatten()[1:],:]
    sum_similar_users = df_pivot.iloc[ind.flatten()[1:],:].sum().sort_values(ascending=False)
    recommendation_pr = []
    df_dict = df_pivot.loc[user_keynum].value.to_dict()
    df_policy_index = [k for k, v in df_dict.items() if v == 1]
    for i in range(len(sum_similar_users.index)):
        policy_num = int(sum_similar_users.index[i][1])
        if policy_num not in df_policy_index:     
            recommendation_pr.append(policy_num) 
            
        if len(recommendation_pr) > k+1:
            break
    print("recommendation_pr",recommendation_pr[:k])
    
    def makeDictFactory(cursor):
        columnNames = [d[0] for d in cursor.description]
        def createRow(*args):
            return dict(zip(columnNames, args))
        return createRow
    print(recommendation_pr[0],recommendation_pr[1])
    
    cursor.execute("SELECT b.*, v.VIEW_SCRAPCNT FROM TB_POLICY b, TB_VIEWSTATS v WHERE b.POLICY_KEYNUM = v.POLICY_KEYNUM and b.POLICY_KEYNUM IN ("+str(recommendation_pr[0])+","+str(recommendation_pr[1])+","+str(recommendation_pr[2])+","+str(recommendation_pr[4])+")")
    cursor.rowfactory = makeDictFactory(cursor)
    data = json.dumps(cursor.fetchall())
    print(data)
    
    # 연결 종료
    cursor.close()
    conn.close()
    return data

@app.route("/user", methods=["GET"])
def curating_for_userinfo():
    k = request.args.get('k')
    k = int(k)
    gender_col = request.args.get('gender_col')
    age_col = request.args.get('age_col')
    education_col = request.args.get('education_col')
    employment_col = request.args.get('employment_col')
   
    conn = cx_Oracle.connect('HR/a1234@localhost:1521/xe')
    # print(conn.version)
    # SQL 쿼리 실행
    cursor = conn.cursor()
    cursor.execute('select * from tb_viewstats')
    
    df = pd.DataFrame(cursor.fetchall(),columns=['VIEW_KEYNUM','POLICY_KEYNUM','VIEW_GENDERCNT_M','VIEW_GENDERCNT_F','VIEW_AGECNT_10','VIEW_AGECNT_20','VIEW_AGECNT_30', 'VIEW_AGECNT_40', 'VIEW_AGECNT_50','VIEW_AGECNT_60','VIEW_EDUCNT_LESHSC','VIEW_EDUCNT_HSCGDT', 'VIEW_EDUCNT_CLG', 'VIEW_EDUCNT_PLTCLGGDT', 'VIEW_EDUCNT_CLGGDT', 'VIEW_EDUCNT_MSTNPHD', 'VIEW_EMPLOYMENT', 'VIEW_UNEMPLOYMENT','VIEW_CNT', 'VIEW_SCRAPCNT', 'VIEW_ETC', 'POLICY_ID'])
    df = df.fillna(0)
    # print('df',df)
    weight = 0.25

    df["recommend_score"] = df[gender_col]* weight + df[age_col] * weight + df[education_col] * weight + df[employment_col] * weight
    recommended_policy = df.sort_values(by="recommend_score", ascending=False)
    recommendation_pr = recommended_policy[:k]['POLICY_KEYNUM'].values
    print(recommendation_pr)
    

    def makeDictFactory(cursor):
        columnNames = [d[0] for d in cursor.description]
        def createRow(*args):
            return dict(zip(columnNames, args))
        return createRow
    
    cursor.execute("SELECT b.*, v.VIEW_SCRAPCNT FROM TB_POLICY b, TB_VIEWSTATS v WHERE b.POLICY_KEYNUM = v.POLICY_KEYNUM and b.POLICY_KEYNUM IN ("+str(recommendation_pr[0])+","+str(recommendation_pr[1])+","+str(recommendation_pr[2])+","+str(recommendation_pr[3])+")")
    cursor.rowfactory = makeDictFactory(cursor)
    data = json.dumps(cursor.fetchall())
    print(data)
    
    # 연결 종료
    cursor.close()
    conn.close()
    return data



if __name__ == '__main__':
    app.run('0.0.0.0', port=5000, debug=True)