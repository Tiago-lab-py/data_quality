# Configuration settings
DATA_PATHS = {
    'raw': 'data/raw',
    'processed': 'data/processed',
    'output': 'data/output'
}

QUALITY_RULES = {
    'null_threshold': 0.05,  # Max 5% nulls allowed
    'duplicate_threshold': 0.01  # Max 1% duplicates allowed
}

# Add more settings as needed