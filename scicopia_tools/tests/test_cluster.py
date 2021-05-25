from scicopia_tools.arangofetch import split_batch

def test_split_batch_complete():
    expected = list(range(10))
    result = []
    for x in split_batch(range(10), 3):
        result.extend(x)
    assert result == expected

def test_split_batch_splits():
    expected = [[0,1,2],[3,4,5],[6,7,8],[9]]
    result = []
    for x in split_batch(range(10), 3):
        result.append(list(x))
    assert result == expected