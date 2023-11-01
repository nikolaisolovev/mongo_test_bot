import calendar
from datetime import datetime, timedelta

from pymongo import MongoClient


def get_aggregated_data(dt_from, dt_upto, group_type):

    client = MongoClient('mongodb://127.0.0.1:27017')
    db = client['my_database']
    collection = db['sample_collection']

    group_types = {
        'hour': '%Y-%m-%dT%H',
        'day': '%Y-%m-%d',
        'month': '%Y-%m'
    }

    dt_format = group_types[group_type]

    query = [
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {"$group": {
            "_id": {"$dateToString": {"format": dt_format, "date": "$dt"}},
            "sum_value": {"$sum": '$value'}}},
        {"$sort": {"_id": 1}},
    ]

    cursor = collection.aggregate(query)

    labels = []
    data = []

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

    result_data = []
    result_labels = []

    current_date = dt_from

    while current_date <= dt_upto:

        if group_type == 'hour':
            delta = timedelta(hours=1)
        elif group_type == 'day':
            delta = timedelta(days=1)
        elif group_type == 'month':
            _, days_in_month = calendar.monthrange(current_date.year, current_date.month)
            delta = timedelta(days=days_in_month)

        result_labels.append(datetime.isoformat(current_date))

        if datetime.isoformat(current_date) not in labels:
            result_data.append(0)
        else:
            value_index = labels.index(datetime.isoformat(current_date))
            result_data.append(data[value_index])

        current_date += delta

    return {'dataset': result_data, 'labels': result_labels}



# dt_from = datetime(2022, 9, 1, 0, 0, 0)
# dt_upto = datetime(2022, 12, 31, 23, 59, 0)
# group_type = "month"
#
#
# print(get_aggregated_data(dt_from, dt_upto, group_type))
