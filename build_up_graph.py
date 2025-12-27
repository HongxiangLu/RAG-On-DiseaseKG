import py2neo
from tqdm import tqdm
import argparse
from collections import defaultdict
import json  # <--- 新增
import os

# ================= 配置区 =================
BATCH_SIZE = 3000  # 每批次处理的大小，建议 1000-5000 之间

# ================= 核心工具函数 =================
def create_indexes(graph, labels):
    """
    为所有标签的'名称'属性创建唯一索引/约束。
    这对于加快 match 速度至关重要，否则插入关系会非常慢。
    """
    print("正在创建索引以加速查询...")
    for label in labels:
        # 在 py2neo 中，通常使用 Cypher 创建约束（包含索引功能）
        # Neo4j 4.x/5.x 语法：
        query = f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.名称 IS UNIQUE"
        try:
            graph.run(query)
        except Exception as e:
            # 如果是旧版本 Neo4j，语法可能不同，或者约束已存在
            print(f"创建索引警告 ({label}): {e}")


def batch_run(graph, query, data, desc="处理中"):
    """
    通用批量执行函数
    :param graph: py2neo Graph对象
    :param query: 包含 UNWIND $batch AS row 的 Cypher 语句
    :param data: 字典列表
    """
    if not data:
        return

    total = len(data)
    for i in tqdm(range(0, total, BATCH_SIZE), desc=desc):
        batch = data[i: i + BATCH_SIZE]
        graph.run(query, batch=batch)


# ================= 导入逻辑 =================
def import_entity_batch(client, label, entity_list):
    """
    批量导入普通实体
    """
    # 使用 MERGE 避免重复，如果确定是新库可以用 CREATE
    query = f"""
    UNWIND $batch AS name
    MERGE (n:{label} {{名称: name}})
    """
    # entity_list 是一个字符串列表 ['药A', '药B'...]
    batch_run(client, query, entity_list, desc=f"导入 {label}")


def import_disease_data_batch(client, disease_list):
    """
    批量导入疾病实体（属性较多）
    """
    query = """
    UNWIND $batch AS row
    MERGE (n:疾病 {名称: row.名称})
    SET n += {
        疾病简介: row.疾病简介,
        疾病病因: row.疾病病因,
        预防措施: row.预防措施,
        治疗周期: row.治疗周期,
        治愈概率: row.治愈概率,
        疾病易感人群: row.疾病易感人群
    }
    """
    batch_run(client, query, disease_list, desc="导入 疾病")


def create_relationship_batch(client, all_relationship):
    """
    批量导入关系
    难点：Cypher 不支持动态 Label (如 MATCH (n:$label))，
    所以必须在 Python 端按 (Label1, Relation, Label2) 分组。
    """
    print("正在对关系数据进行分组预处理...")

    # 分组结构: key=(type1, relation, type2), value=[{name1:..., name2:...}, ...]
    grouped_rels = defaultdict(list)

    for type1, name1, relation, type2, name2 in all_relationship:
        key = (type1, relation, type2)
        grouped_rels[key].append({"n1": name1, "n2": name2})

    print(f"关系分组完成，共 {len(grouped_rels)} 种关系类型。开始批量导入...")

    for (type1, rel_type, type2), data_list in grouped_rels.items():
        # 动态构建 Cypher
        query = f"""
        UNWIND $batch AS row
        MATCH (a:{type1} {{名称: row.n1}})
        MATCH (b:{type2} {{名称: row.n2}})
        MERGE (a)-[r:{rel_type}]->(b)
        """
        batch_run(client, query, data_list, desc=f"连线 {type1}->{rel_type}->{type2}")


# ================= 主程序 =================
if __name__ == "__main__":
    # --- 新增：读取配置文件逻辑 ---
    config_data = {}
    config_path = "Neo4j_Config.json"

    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
    else:
        print(f"警告: 未找到 {config_path}，将仅依赖命令行参数。")

    parser = argparse.ArgumentParser(description="通过medical_new.json文件,创建一个知识图谱")

    # --- 修改：将 default 值改为从 config_data 中获取，如果没有则为 None ---
    parser.add_argument('--website', type=str, default=config_data.get('website'), help='neo4j的连接网站')
    parser.add_argument('--user', type=str, default=config_data.get('user'), help='neo4j的用户名')
    parser.add_argument('--password', type=str, default=config_data.get('password'), help='neo4j的密码')
    parser.add_argument('--dbname', type=str, default=config_data.get('dbname', 'neo4j'), help='数据库名称')

    args = parser.parse_args()

    # 检查必要参数是否为空（防止配置文件和命令行都没提供的情况）
    if not all([args.website, args.user, args.password]):
        print("错误: 缺少数据库连接配置。请检查 config.json 或通过命令行参数传入。")
        exit(1)

    # 1. 连接数据库
    try:
        # 这里直接使用 args 即可，因为它们已经包含了配置文件或命令行的值
        client = py2neo.Graph(args.website, user=args.user, password=args.password, name=args.dbname)
        print(f"数据库连接成功: {args.website}")
    except Exception as e:
        print(f"数据库连接失败: {e}")
        exit(1)

    # 2. 清空数据库
    print('删除neo4j上的所有实体......')
    client.run("MATCH (n) DETACH DELETE (n)")

    # 3. 数据处理 (保持原有逻辑，稍作清理)
    print("正在读取并解析 JSON 数据...")
    with open('data/medical_new.json', 'r', encoding='utf-8') as f:
        all_data = f.read().split('\n')

    all_entity = {
        "疾病": [], "药品": [], "食物": [], "检查项目": [],
        "科目": [], "疾病症状": [], "治疗方法": [], "药品商": [],
    }

    relationship = []

    for data_str in tqdm(all_data, desc="解析原始数据"):
        if len(data_str) < 3:
            continue
        try:
            data = eval(data_str[:-1])  # 使用 eval 解析每行
        except:
            continue

        disease_name = data.get("name", "")
        all_entity["疾病"].append({
            "名称": disease_name,
            "疾病简介": data.get("desc", ""),
            "疾病病因": data.get("cause", ""),
            "预防措施": data.get("prevent", ""),
            "治疗周期": data.get("cure_lasttime", ""),
            "治愈概率": data.get("cured_prob", ""),
            "疾病易感人群": data.get("easy_get", ""),
        })

        # --- 提取实体和关系逻辑 (保持不变) ---
        drugs = data.get("common_drug", []) + data.get("recommand_drug", [])
        all_entity["药品"].extend(drugs)
        if drugs:
            relationship.extend([("疾病", disease_name, "疾病使用药品", "药品", drug) for drug in drugs])

        do_eat = data.get("do_eat", []) + data.get("recommand_eat", [])
        no_eat = data.get("not_eat", [])
        all_entity["食物"].extend(do_eat + no_eat)
        if do_eat:
            relationship.extend([("疾病", disease_name, "疾病宜吃食物", "食物", f) for f in do_eat])
        if no_eat:
            relationship.extend([("疾病", disease_name, "疾病忌吃食物", "食物", f) for f in no_eat])

        check = data.get("check", [])
        all_entity["检查项目"].extend(check)
        if check:
            relationship.extend([("疾病", disease_name, "疾病所需检查", "检查项目", ch) for ch in check])

        cure_department = data.get("cure_department", [])
        all_entity["科目"].extend(cure_department)
        if cure_department:
            relationship.append(("疾病", disease_name, "疾病所属科目", "科目", cure_department[-1]))

        symptom = data.get("symptom", [])
        # 简单清理 symptom
        symptom = [sy[:-3] if sy.endswith('...') else sy for sy in symptom]
        all_entity["疾病症状"].extend(symptom)
        if symptom:
            relationship.extend([("疾病", disease_name, "疾病的症状", "疾病症状", sy) for sy in symptom])

        cure_way = data.get("cure_way", [])
        if cure_way:
            # 简单清理 cure_way
            clean_cure_way = []
            for w in cure_way:
                if isinstance(w, list): w = w[0]
                if w and len(str(w)) >= 2: clean_cure_way.append(w)

            all_entity["治疗方法"].extend(clean_cure_way)
            relationship.extend([("疾病", disease_name, "治疗的方法", "治疗方法", w) for w in clean_cure_way])

        acompany_with = data.get("acompany", [])
        if acompany_with:
            relationship.extend([("疾病", disease_name, "疾病并发疾病", "疾病", d) for d in acompany_with])

        drug_detail = data.get("drug_detail", [])
        for detail in drug_detail:
            if ',' in detail:
                lis = detail.split(',')
                if len(lis) == 2:
                    p, d = lis[0], lis[1]
                    all_entity["药品商"].append(d)
                    all_entity["药品"].append(p)
                    relationship.append(('药品商', d, "生产", "药品", p))

    # 去重
    relationship = list(set(relationship))
    # 实体去重 (保持 '疾病' 列表不变，因为它是 dict list，其他是 str list)
    for k, v in all_entity.items():
        if k != "疾病":
            all_entity[k] = list(set(v))

    # 4. 创建索引 (优化关键点!)
    # 收集所有标签
    all_labels = list(all_entity.keys())
    create_indexes(client, all_labels)

    # 5. 写入数据到 Neo4j
    print("开始向 Neo4j 写入数据...")

    for k in all_entity:
        if k != "疾病":
            import_entity_batch(client, k, all_entity[k])
        else:
            import_disease_data_batch(client, all_entity[k])

    # 6. 写入关系
    create_relationship_batch(client, relationship)

    print("全部导入完成！")