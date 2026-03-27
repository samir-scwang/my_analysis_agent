from pprint import pprint

from app.graphs.main_graph import build_graph

def main():
    graph = build_graph()
    initial_state = {
        "request_id": "req_00",
        "session_id": "sess_004",
        "user_id": "user_004",
        "dataset_id": "ds_004",
        "dataset_path": "./data/demo_sales.csv",
        "user_prompt": "请做一份详细且图表丰富的销售分析报告",
        "input_config": {
            "language": "zh-CN",
            "output_format": ["markdown"],
        },
        "memory_context": {},
        "revision_round": 0,
        "max_review_rounds": 2,
        "revision_tasks": [],
        "revision_context": {},
        "execution_mode": "normal",
        "warnings": [],
        "errors": [],
        "status": "INIT",
        "degraded_output":False
    }
    for state in graph.stream(initial_state, stream_mode="updates"):

        # print("status:", state.get("status"))
        # print("state", state)
        pprint(state)
    #
    # result = graph.invoke(initial_state)
    # pprint(result)


if __name__ == "__main__":
    main()
