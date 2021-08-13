from ipfs_indexer.procs import extract_information
import pytest


@pytest.mark.skip(reason="requires external ipfs gateway to be online")
def test_should_recognize():
    cid = "QmXDs15TwsXWotqm6aqV5VCABoNRBRHwbXMSSmR4uKh8HG"
    result = extract_information(cid)
    print(result)
    assert False
