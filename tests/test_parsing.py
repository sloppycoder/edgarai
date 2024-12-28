from edgar.filing import SECFiling
from edgar.util import (
    download_file,
    trim_html_content,
)


def test_parse_485bpos_filing():
    filing = SECFiling("1002427", "edgar/data/1002427/0001133228-24-004879.txt")
    html_filename = filing.get_doc_by_type("485BPOS")[0]
    doc_path = download_file(html_filename)
    assert doc_path

    content = trim_html_content(doc_path)

    assert filing.date_filed == "2024-04-29"
    assert filing.accession_number == "0001133228-24-004879"
    assert len(filing.documents) == 26
    assert (
        html_filename == "edgar/data/1002427/000113322824004879/msif-html7854_485bpos.htm"
    )
    assert "hidden" not in content
    assert "FORM N-1A" in content
