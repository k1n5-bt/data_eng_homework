import os
import pandas as pd
import numpy as np
import json


def write_file(data, filename):
    with open(filename, mode='w', encoding='utf-8') as f:
        json.dump(data, f, default=str, ensure_ascii=False)


def mem_usage(df):
    ub = df.memory_usage(deep=True)
    if isinstance(df, pd.DataFrame):
        ub = ub.sum()

    umb = ub / 1024 ** 2
    return umb


def get_dict_by_name(lst, key):
    for d in lst:
        if d['column_name'] == key:
            return d

    return {}


def estimate_memory(df, file, accum: dict = {}):
    d = {}
    file_size = os.path.getsize(file)
    mem_stat = df.memory_usage(deep=True)

    acc_col_stat = accum.get('column_stats', [])

    acc_type_stat = accum.get('types_stat', [])

    total_mem_usage = mem_stat.sum()

    d['file_size'] = int(file_size) // 1024

    d['in_memory_size'] = int(total_mem_usage) // 1024 + accum.get('in_memory_size', 0)

    column_stats = []

    for key in df.dtypes.keys():
        acc_col_stat_d = get_dict_by_name(acc_col_stat, key)
        total_mem_stat = int(mem_stat[key]) + acc_col_stat_d.get('memory_abs', 0)
        column_stats.append(
            {
                'column_name': key,
                'memory_abs': int(total_mem_stat) // 1024,
                'memory_percent': round((total_mem_stat / (d['in_memory_size'] * 1024)) * 100, 4),
                'dtype': df.dtypes[key]
            }
        )

    column_stats.sort(key=lambda x: x['memory_abs'], reverse=True)

    d['shape'] = {}
    d['shape']['0'] = df.shape[0]
    d['shape']['1'] = df.shape[1]

    acc_shape = accum.get('shape', None)
    if acc_shape:
        d['shape']['0'] += acc_shape['0']

    types_stats = []
    for dtype in ['float', 'int', 'object']:
        selected_dtype = df.select_dtypes(include=[dtype])
        mem_usage_bytes = selected_dtype.memory_usage(deep=True).mean()

        acc_type_stat_d = get_dict_by_name(acc_type_stat, dtype)

        mem_usage_mb = round(mem_usage_bytes / 1024 ** 2, 4) + acc_type_stat_d.get('size:MB', 0)
        types_stats.append({
            'column_name': dtype,
            'size:MB': mem_usage_mb
        })

    d['types_stat'] = types_stats
    d['column_stats'] = column_stats

    return d


def convert_object_datatypes(df, accum={}):
    conv_df = pd.DataFrame()
    df_obj = df.select_dtypes(include=['object']).copy()

    for col in df_obj.columns:
        num_unique_values = len(df_obj[col].unique())
        num_total_values = len(df_obj[col])
        if (num_unique_values / num_total_values) < 0.5:
            conv_df.loc[:, col] = df_obj[col].astype('category')
        else:
            conv_df.loc[:, col] = df_obj[col]

    d = {}
    oacsz = accum.get("objects_size", 0)
    oacast = accum.get("objects_astype", 0)

    d['objects_size'] = round(mem_usage(df_obj), 2) + oacsz
    d['objects_astype'] = round(mem_usage(conv_df), 2) + oacast

    return conv_df, d


def int_downcast(df, accum={}):

    df_int = df.select_dtypes(include=['int'])
    df_int_downcast = df_int.apply(pd.to_numeric, downcast='unsigned')

    compare_ints = pd.concat([df_int.dtypes, df_int_downcast.dtypes], axis=1)
    compare_ints.columns = ['before', 'after']
    compare_ints.apply(pd.Series.value_counts)

    d = {}

    oacsz = accum.get('df_int_size', 0)
    oacast = accum.get('df_int_downcast_size', 0)

    d['df_int_size'] = round(mem_usage(df_int), 2) + oacsz
    d['df_int_downcast_size'] = round(mem_usage(df_int_downcast), 2) + oacast
    d['type_conversion'] = compare_ints.to_dict()

    return df_int_downcast, d


def float_downcast(df, accum={}):
    df_float = df.select_dtypes(include=['float'])
    df_float_downcast = df_float.apply(pd.to_numeric, downcast='float')
    compare_floats = pd.concat([df_float.dtypes, df_float_downcast.dtypes], axis = 1)
    compare_floats.columns = ['before', 'after']
    compare_floats.apply(pd.Series.value_counts)

    d = {}

    oacsz = accum.get('df_float_size',0)
    oacast = accum.get('df_float_downcast_size', 0)

    d['df_float_size'] = round(mem_usage(df_float), 2) + oacsz
    d['df_float_downcast_size'] = round(mem_usage(df_float_downcast), 2) + oacast
    d['type_conversion'] = compare_floats.to_dict()

    return df_float_downcast, d


def chunks_read(filename, cols, types_list, out_filename, ch_size=10000):
    if os.path.isfile(out_filename):
        os.remove(out_filename)

    has_header = True
    for chunk in pd.read_csv(filename, usecols=lambda x: x in cols, dtype=types_list, chunksize=ch_size):
        print(f"chunk_mem {mem_usage(chunk)}")

        chunk.to_csv(out_filename, mode="a", header=has_header)
        has_header = False


def read_pandas_types(filename):
    with open(filename, mode="r") as f:
        types = json.load(f)

        for key in types.keys():
            if types[key] == 'category':
                types[key] = pd.CategoricalDtype()
            else:
                types[key] = np.dtype(types[key])

        return types
