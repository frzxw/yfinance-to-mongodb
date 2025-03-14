def sanitize_data(data):
    if isinstance(data, pd.DataFrame):
        return json.loads(data.reset_index().to_json(orient='records', date_format='iso'))
    elif isinstance(data, pd.Series):
        return json.loads(data.to_json(orient='index', date_format='iso'))
    elif isinstance(data, dict):
        return {k: sanitize_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_data(item) for item in data]
    return convert_timestamps(data)

def convert_timestamps(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif hasattr(obj, 'to_dict'):
        return obj.to_dict()
    elif pd.isna(obj):
        return None
    return obj