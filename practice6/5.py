from pandas import read_csv, set_option
import seaborn as sns
import matplotlib.pyplot as plt
from common import estimate_memory, write_file, convert_object_datatypes, int_downcast, float_downcast, mem_usage, chunks_read, read_pandas_types

set_option("display.max_rows", 20, "display.max_columns", 200)

filename = "data/5/dataset.csv"

types_file = "out/5/df_types.json"
opt_datafile_name = "data/5/asteroid_optcols.csv"
chunk_filename = "data/5/asteroid_df_chunk.csv"

selected_columns = ['spkid', 'full_name', 'diameter', 'albedo', 'epoch', 'equinox',
                    'tp', 'per', 'moid', 'class', 'rms']


def get_stat_and_optimize(datafile, chzs=None):
    md = {}
    md_opt = {}
    conv_d_obj = {}
    conv_d_int = {}
    conv_d_float = {}
    for df_chunk in read_csv(datafile, chunksize=chzs):
        md = estimate_memory(df_chunk, datafile, md)

        df_optimized = df_chunk.copy()

        df_obj, conv_d_obj = convert_object_datatypes(df_chunk, conv_d_obj)
        df_int, conv_d_int = int_downcast(df_chunk, conv_d_int)
        df_float, conv_d_float = float_downcast(df_chunk, conv_d_float)

        df_optimized[df_int.columns] = df_int
        df_optimized[df_float.columns] = df_float
        df_optimized[df_obj.columns] = df_obj

        print(mem_usage(df_chunk))
        print(mem_usage(df_optimized))

        md_opt = estimate_memory(df_optimized, datafile, md_opt)

    write_file(conv_d_obj, "out/5/objects_memopt.json")
    write_file(conv_d_int, "out/5/int_downcast.json")
    write_file(conv_d_float, "out/5/float_downcast.json")

    write_file(md, "out/5/memusage_noopt.json")
    write_file(md_opt, "out/5/memusage_optimized.json")

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
    df_plot.info(memory_usage='deep')

    #1
    sns.pairplot(df_plot.select_dtypes(include=['float32'])).savefig("out/5/pairplot.png")

    #2
    df_g = df_plot.groupby(['class'])[['diameter']].agg({'diameter': ['min', 'max', 'mean']})

    plot = df_g.plot(title="class by diameter")
    plot.get_figure().savefig("out/5/diameter_by_class.png")

    #3
    fig, ax = plt.subplots()

    sns.scatterplot(data=df_plot, x='epoch', y='class')

    plt.savefig("out/5/total.png")

    #4
    fig, ax = plt.subplots()

    sns.scatterplot(data=df_plot, x='diameter', y='moid')

    plt.savefig("out/5/diameter_by_moid.png")
