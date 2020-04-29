import json
import os

STORE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "res")


def get_tmp0_data():
    CURRENT_DIR = os.path.join(STORE_DIR, "tmp0")
    tmp0_json_list = ["tmp0_afternoon.json", "tmp0_evening.json",
                      "tmp0_midnight.json", "tmp0_morning.json"]
    raw_data = []

    for json_name in tmp0_json_list:
        with open(os.path.join(CURRENT_DIR, json_name)) as f:
            res = json.load(f)
            for i in range(len(res["_1"])):
                raw_data.append((int(res["_1"][str(i)]), res["_2"][str(i)]))
            del res
    raw_data.sort()
    res = list(zip(*raw_data))
    del raw_data
    pretty_res = {"label": [str(i) + ":00" for i in list(res[0])],
                  "data": list(res[1])}

    return pretty_res


def get_tmp1_data():
    type_json_file = os.path.join(STORE_DIR, "tmp1", "type.json")
    with open(type_json_file) as f:
        res = json.load(f)

    raw_data = []
    for i in range(len(res["_1"])):
        raw_data.append((res["_1"][str(i)], res["_2"][str(i)]))
    del res
    res = list(zip(*raw_data))
    del raw_data
    pretty_res = {"label": list(res[0]),
                  "data": list(res[1])}
    return pretty_res

def get_tmp3_data():
    type_json_file = os.path.join(STORE_DIR, "tmp3", "region.json")
    with open(type_json_file) as f:
        res = json.load(f)

    raw_data = []
    for i in range(len(res["_1"])):
        if res["_1"][str(i)] == "US":
            continue
        raw_data.append((res["_1"][str(i)], res["_2"][str(i)]))
    del res

    res = list(zip(*raw_data))
    del raw_data
    pretty_res = {"label": list(res[0]),
                  "data": list(res[1])}
    return pretty_res

if __name__ == '__main__':
    print(STORE_DIR)
    print("tmp0:")
    print(get_tmp0_data())

    print("tmp1:")
    print(get_tmp1_data())

    print("tmp3:")
    print(get_tmp3_data())
