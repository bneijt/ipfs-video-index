import pytest
from ipfs_video_index.ipfs_indexer.procs import extract_information, extract_names


@pytest.mark.skip(reason="requires external ipfs gateway to be online")
def test_should_recognize():
    cid = "QmXDs15TwsXWotqm6aqV5VCABoNRBRHwbXMSSmR4uKh8HG"
    result = extract_information(cid)
    print(result)
    assert False


def test_direct_links_regex():
    content = """<a class="ipfs-hash" href="/ipfs/QmUsxG6i5XsCGu4dJZcvvyMbsXrUt5sgL6H2JwGmtBhHqW?filename=01_llama_drama_1080p.webm">
            QmUs…hHqW
          </a>"""
    assert extract_names(content) == {
        "QmUsxG6i5XsCGu4dJZcvvyMbsXrUt5sgL6H2JwGmtBhHqW": "01_llama_drama_1080p.webm"
    }
