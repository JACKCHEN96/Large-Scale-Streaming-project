import json
import os

import pandas as pd

STORE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "res")


def get_tmp0_data():
    tmp0_json_file = os.path.join(STORE_DIR, "tmp0", "tmp0.json")
    raw_data = []
    with open(tmp0_json_file) as f:
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


def get_form_data(people_id="", tags=None, day="", clock=""):
    def helper(file_name):
        with open(file_name) as f:
            res = json.load(f)

        raw_data = []
        for i in range(len(res["_1"])):
            raw_data.append((res["_1"][str(i)], res["_2"][str(i)]))
        del res

        res = list(zip(*raw_data))
        del raw_data
        pretty_res = [list(res[0]), list(res[1])]
        return pretty_res

    tag_json_file = os.path.join(STORE_DIR, "tmp2", "pptag.json")
    type_json_file = os.path.join(STORE_DIR, "tmp2", "pptype.json")
    clock_json_file = os.path.join(STORE_DIR, "tmp5", "clock.json")
    day_json_file = os.path.join(STORE_DIR, "tmp5", "day.json")

    tag_list = helper(tag_json_file)
    type_list = helper(type_json_file)
    clock_list = helper(clock_json_file)
    day_list = helper(day_json_file)

    tag_df = pd.DataFrame({"people-id": tag_list[0],
                           "tag": tag_list[1]
                           })
    type_df = pd.DataFrame({"people-id": type_list[0],
                            "type": type_list[1]})
    clock_df = pd.DataFrame({"people-id": clock_list[0],
                             "clock": clock_list[1]})
    day_df = pd.DataFrame({"people-id": day_list[0],
                           "day": day_list[1]})

    tmp1_df = pd.merge(tag_df, type_df, how="outer", on="people-id")
    tmp2_df = pd.merge(clock_df, day_df, how="outer", on="people-id")

    tmp_df = pd.merge(tmp1_df, tmp2_df, how="outer", on="people-id")
    # apply filter conditions
    if not people_id == "":
        tmp_df = tmp_df.loc[tmp_df["people-id"] == people_id]
    if not tags is None:
        if len(tags) > 0:
            tmp_df = tmp_df.loc[tmp_df["tag"].isin(tags)]
    if not day == "":
        tmp_df = tmp_df.loc[tmp_df["day"] == day]
    if not clock == "":
        tmp_df = tmp_df.loc[tmp_df["clock"] == clock]

    pretty_res = json.loads(tmp_df.to_json(orient="split"))
    pretty_res["columns"] = [{"title": col} for col in pretty_res["columns"]]
    return pretty_res


if __name__ == '__main__':
    print(STORE_DIR)
    # print("tmp0:")
    # print(get_tmp0_data())
    #
    # print("tmp1:")
    # print(get_tmp1_data())
    #
    # print("tmp3:")
    # print(get_tmp3_data())
    print(get_form_data(day="Fri"))
    print(get_form_data(tags=None))
    print(get_form_data(tags=[]))
    print(get_form_data(tags=["Job", "Private"]))
