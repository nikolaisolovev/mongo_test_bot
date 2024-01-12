import calendar
from datetime import datetime, timedelta

from pymongo import MongoClient


def get_aggregated_data(dt_from, dt_upto, group_type):
    """
    Aggregate data from a MongoDB collection based on specified time range and grouping type.

    Parameters:
    - dt_from (datetime): The starting datetime for the data retrieval.
    - dt_upto (datetime): The ending datetime for the data retrieval.
    - group_type (str): The type of time grouping ('hour', 'day', 'month').

    Returns:
    dict: A dictionary containing aggregated data with 'dataset' and 'labels'.
          'dataset' represents the aggregated values, and 'labels' represents the corresponding time labels.
    """

    # Connect to MongoDB
    client = MongoClient('mongodb://127.0.0.1:27017')
    db = client['my_database']
    collection = db['sample_collection']

    # Define date format based on group_type
    group_types = {
        'hour': '%Y-%m-%dT%H',
        'day': '%Y-%m-%d',
        'month': '%Y-%m'
    }

    dt_format = group_types[group_type]

    # Define date format based on group_type
    query = [
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {"$group": {
            "_id": {"$dateToString": {"format": dt_format, "date": "$dt"}},
            "sum_value": {"$sum": '$value'}}},
        {"$sort": {"_id": 1}},
    ]

    cursor = collection.aggregate(query)

    # Process MongoDB cursor results
    labels = []
    data = []

    # Define ISO format based on group_type
    iso_types = {
        'hour': ':00:00',
        'day': 'T00:00:00',
        'month': '-01T00:00:00'
    }

    iso_format = iso_types[group_type]

    for doc in cursor:
        dt_raw = datetime.fromisoformat(doc['_id'] + iso_format)
        dt_iso = datetime.isoformat(dt_raw)
        labels.append(dt_iso)
        data.append(doc['sum_value'])

    # Create a consistent time range with labels and corresponding data
    result_data = []
    result_labels = []

    current_date = dt_from

    while current_date <= dt_upto:
        # Determine delta based on group_type
        if group_type == 'hour':
            delta = timedelta(hours=1)
        elif group_type == 'day':
            delta = timedelta(days=1)
        elif group_type == 'month':
            _, days_in_month = calendar.monthrange(current_date.year, current_date.month)
            delta = timedelta(days=days_in_month)

        # Populate result_labels and result_data based on the time range
        result_labels.append(datetime.isoformat(current_date))

        if datetime.isoformat(current_date) not in labels:
            result_data.append(0)
        else:
            value_index = labels.index(datetime.isoformat(current_date))
            result_data.append(data[value_index])

        current_date += delta

    return {'dataset': result_data, 'labels': result_labels}
