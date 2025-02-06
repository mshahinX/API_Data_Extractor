import pytest
from data_extractor import MSISDNRequestExtractor

def test_url_validation():
    # Test valid URL
    extractor = MSISDNRequestExtractor("https://example.com/api")
    assert extractor.base_url == "https://example.com/api"

    # Test invalid URL should raise ValueError
    with pytest.raises(ValueError):
        MSISDNRequestExtractor("invalid-url")

def test_msisdn_validation():
    extractor = MSISDNRequestExtractor("https://example.com/api")
    
    # Test valid MSISDN processing
    result = extractor.extract_data(
        {"accounts": [{"accountInternalId": "123"}]}, 
        ["accounts.accountInternalId"], 
        "1234567890"
    )
    assert result['msisdn'] == "1234567890"
    assert result['accounts.accountInternalId'] == "123"

    # Test with None data
    result = extractor.extract_data(None, ["test.key"], "1234567890")
    assert result['msisdn'] == "1234567890"
    assert result['test.key'] is None