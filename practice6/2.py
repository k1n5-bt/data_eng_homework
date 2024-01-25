from pandas import read_csv, to_datetime, set_option
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from common import estimate_memory, write_file, convert_object_datatypes, int_downcast, float_downcast, mem_usage, chunks_read, read_pandas_types

set_option("display.max_rows", 20, "display.max_columns", 200)

filename = "data/2/automotive.csv"

types_file = "out/2/df_types.json"
opt_datafile_name = "out/2/automotive_optcols.csv"
chunk_filename = "out/2/automotive_df_chunk.csv"

selected_columns = ['firstSeen', 'brandName', 'modelName', 'vf_TransmissionStyle', 'askPrice',
                    'isNew', 'vf_Series', 'vf_WheelSizeRear', 'vf_Seats']


def get_stat_and_optimize(datafile, chzs=None, nr=None):
    md = {}
    md_opt = {}
    conv_d_obj = {}
    conv_d_int = {}
    conv_d_float = {}

    total_mem_usage = 0
    total_optimal_memusage = 0

    for df_chunk in read_csv(datafile, chunksize=chzs, nrows=nr, low_memory=False):
        print("read chunk")
        md = estimate_memory(df_chunk, datafile, md)
        #df_chunk.info()

        df_optimized = df_chunk.copy()

        df_obj, conv_d_obj = convert_object_datatypes(df_chunk, conv_d_obj)
        df_int, conv_d_int = int_downcast(df_chunk, conv_d_int)
        df_float, conv_d_float = float_downcast(df_chunk, conv_d_float)

        df_optimized[df_int.columns] = df_int
        df_optimized[df_float.columns] = df_float
        df_optimized[df_obj.columns] = df_obj

        total_mem_usage += mem_usage(df_chunk)
        total_optimal_memusage += mem_usage(df_optimized)

        md_opt = estimate_memory(df_optimized, datafile, md_opt)

    print(f"Mem usage {total_mem_usage} \nOPT {total_optimal_memusage}")

    write_file(conv_d_obj, "out/2/objects_memopt.json")
    write_file(conv_d_int, "out/2/int_downcast.json")
    write_file(conv_d_float, "out/2/float_downcast.json")

    write_file(md, f"out/2/memusage_noopt.json")
    write_file(md_opt, f"out/2/memusage_optimized.json")

    opt_dtypes = df_optimized.dtypes
    nc = {}

    for key in selected_columns:
        nc[key] = opt_dtypes[key]

    write_file(nc, types_file)
    read_by_columns = read_csv(datafile, usecols=lambda x: x in selected_columns, dtype=nc)

    print(read_by_columns.shape)
    print("By colums size = ", mem_usage(read_by_columns))

    read_by_columns.to_csv(opt_datafile_name)

    chunks_read(datafile, selected_columns, nc, chunk_filename)


if __name__ == "__main__":
    get_stat_and_optimize(filename, chzs=200_000)

    need_dtypes = read_pandas_types(types_file)
    print(need_dtypes)

    df_plot = read_csv(opt_datafile_name, usecols=lambda x: x in need_dtypes.keys(), dtype=need_dtypes)
    #df_plot.info(memory_usage='deep')

    #1
    sns.pairplot(df_plot).savefig("out/2/pairplot.png")

    #2
    df_n = df_plot.groupby(['brandName'])[['isNew']].count()

    df_n = df_n[(df_n['isNew'] > 100000)]

    plot2 = df_n.plot(title="brands car", rot=90, figsize=(30, 15), kind='bar')
    plot2.get_figure().savefig("out/2/brands.png")

    #3
    plot2 = df_plot['brandName'].head(10).value_counts().plot(kind='pie', title='Brands', autopct='%1.0f%%')
    plot2.get_figure().savefig("out/2/pie_brandName.png")

    #4)
    df_trans = df_plot.groupby(['vf_TransmissionStyle'])[['vf_WheelSizeRear']].agg({'vf_WheelSizeRear': ['max', 'mean', 'min']})

    plt.figure(figsize=(30,5))
    plt.title('WheelSize per Transmission')
    plt.plot(df_trans)
    plt.savefig("out/2/askPrice.png")

    #5)
    plt.figure(figsize=(30, 30))
    gr_obj = df_plot.groupby(["year", "brandName"], as_index=False)['askPrice'].mean()
    gr_obj = gr_obj[gr_obj['askPrice'] < 1000000]

    sns.set_style("ticks", {'axes.grid': True})
    sns.lineplot(data=gr_obj, x="year", y="askPrice", hue="brandName").set(title='Avg pricy per year')
    plt.savefig("out/2/avg_price.png")
