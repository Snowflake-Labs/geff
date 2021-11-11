from lambda_src.utils import pick

PICK_MOCK_DATA = {"a": {"b": {"c": "dummy"}}}


def test_pick_nested_all_keys_exist():
    assert pick("a.b.c", PICK_MOCK_DATA) == "dummy"


def test_pick_missing_none_value_in_path():
    mock_dict = {"a": {"b": None}}
    assert pick("a.b.c", mock_dict) == None


def test_pick_missing_keys_in_path():
    mock_dict = {"a": 1}
    assert pick("a.b.c", mock_dict) == None


def test_pick_bad_path():
    assert pick("a.b..", PICK_MOCK_DATA) == {"c": "dummy"}
