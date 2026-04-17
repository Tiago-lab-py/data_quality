import pytest
import pandas as pd
from lib.quality_rules import apply_quality_rules

def test_apply_quality_rules():
    # Create test data
    df = pd.DataFrame({
        'col1': [1, 2, None, 4],
        'col2': ['a', 'b', 'c', 'd']
    })
    
    results = apply_quality_rules(df)
    
    assert 'null_check' in results
    assert 'duplicate_check' in results
    assert results['null_check']['failed'] == 1  # One null value

def test_check_nulls():
    from lib.quality_rules import check_nulls
    
    df = pd.DataFrame({'a': [1, None], 'b': [2, 3]})
    result = check_nulls(df)
    
    assert result['failed'] == 1
    assert result['passed'] == 3