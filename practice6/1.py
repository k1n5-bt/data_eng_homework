from pandas import Grouper, read_csv, to_datetime, set_option
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from common import estimate_memory, write_file, convert_object_datatypes, int_downcast, float_downcast, mem_usage, chunks_read, read_pandas_types

set_option("display.max_rows", 20, "display.max_columns", 60)

filename = 'data/1/game_logs.csv'

types_file = 'out/1/df_types.json'
opt_datafile_name = "data/1/game_dataset_optcols.csv"
chunk_filename = "data/1/game_dataset_df_chunk.csv"

selected_columns = ['date', 'h_game_number', 'v_score', 'h_score', 'v_at_bats', 'v_hits',
                    'h_homeruns', 'h_passed_balls', 'saving_pitcher_id', 'h_manager_name']


def get_stat_and_optimize(file):
    df = read_csv(file)

    md = estimate_memory(df, file)
    write_file(md, 'out/1/memusage.json')

    df_optimized = df.copy()

    df_obj, conv_d_obj = convert_object_datatypes(df)
    write_file(conv_d_obj, 'out/1/objects_memopt.json')

    df_int, conv_d_int = int_downcast(df)
    write_file(conv_d_int, 'out/1/int_downcast.json')

    df_float, conv_d_float = float_downcast(df)
    write_file(conv_d_float, 'out/1/float_downcast.json')

    df_optimized[df_int.columns] = df_int
    df_optimized[df_float.columns] = df_float
    df_optimized[df_obj.columns] = df_obj

    print(mem_usage(df))
    print(mem_usage(df_optimized))

    md_opt = estimate_memory(df_optimized, file)
    write_file(md_opt, 'out/1/memusage_optimized.json')

    opt_dtypes = df_optimized.dtypes
    nc = {}

    for key in selected_columns:
        nc[key] = opt_dtypes[key]

    write_file(nc, types_file)
    read_by_columns = read_csv(file, usecols=lambda x: x in selected_columns, dtype=nc)

    print(read_by_columns.shape)
    print("By colums size = ", mem_usage(read_by_columns))

    read_by_columns.to_csv(opt_datafile_name)

    chunks_read(file, selected_columns, nc, chunk_filename)


if __name__ == "__main__":
    get_stat_and_optimize(filename)

    need_dtypes = read_pandas_types(types_file)

    print(need_dtypes)

    df_plot = read_csv(opt_datafile_name, usecols=lambda x: x in need_dtypes.keys(), dtype=need_dtypes)

    df_plot.info(memory_usage='deep')

    df_plot['date'] = to_datetime(df_plot['date'], format="%Y%m%d")

    #1
    df_g = df_plot.groupby(Grouper(key = 'date', freq='Y'))['h_homeruns'].count()
    plot = df_g.plot(title="homeruns per year")
    plot.get_figure().savefig("out/1/homeruns.png")

    #2
    df_m = df_plot.groupby(['h_manager_name'])[['v_score']].agg( {'v_score' : ['max', 'mean', 'min']})
    df_m = df_m[df_m[('v_score', 'max')] > 25]

    plot2 = df_m.plot(kind='barh', title="score by manager", figsize=(30,15))
    plot2.get_figure().savefig("out/1/score.png")

    #3
    sns.pairplot(df_plot).savefig("out/1/pairplot.png")

    #4

    fig, ax = plt.subplots()
    sns.scatterplot(data=df_plot, x='h_passed_balls', y='h_score')
    plt.savefig("out/1/passed_score.png")

    #5

    date_from = to_datetime("20000101", format="%Y%m%d")
    date_to = to_datetime("20160101", format="%Y%m%d")

    df_hitmap = df_plot[(df_plot['date'] > date_from) & (df_plot['date'] < date_to)]

    df_hitmap['year'] = df_hitmap['date'].dt.year
    df_hitmap['Month'] = df_hitmap['date'].dt.month

    fig, ax = plt.subplots()

    plt.figure(figsize=(16,6))
    sns.heatmap(df_hitmap.pivot_table(values='v_score', index=['year'], columns=['Month'], aggfunc=np.sum), annot=True, cmap="YlGnBu", cbar=True);
    plt.savefig("out/1/hitmap.png")

    print(df_plot.head())
