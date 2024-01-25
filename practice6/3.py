from pandas import read_csv, set_option
import seaborn as sns
import matplotlib.pyplot as plt
from common import estimate_memory, write_file, convert_object_datatypes, int_downcast, float_downcast, mem_usage, chunks_read, read_pandas_types

set_option("display.max_rows", 20, "display.max_columns", 60)

filename = "data/3/flights.csv"

types_file = "out/3/df_types.json"
opt_datafile_name = "out/3/flights_optcols.csv"
chunk_filename = "out/3/flights_df_chunk.csv"

selected_columns = ['YEAR', 'MONTH', 'DAY', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', 'AIR_TIME',
                    'DISTANCE', 'WHEELS_ON', 'AIRLINE_DELAY', 'WEATHER_DELAY']


def get_stat_and_optimize(datafile):
    df = read_csv(datafile)

    md = estimate_memory(df, datafile)
    write_file(md, f"out/3/memusage_noopt.json")

    df_optimized = df.copy()

    df_obj, conv_d_obj = convert_object_datatypes(df)
    write_file(conv_d_obj, "out/3/objects_memopt.json")

    df_int, conv_d_int = int_downcast(df)
    write_file(conv_d_int, "out/3/int_downcast.json")

    df_float, conv_d_float = float_downcast(df)
    write_file(conv_d_float, "out/3/float_downcast.json")

    df_optimized[df_int.columns] = df_int
    df_optimized[df_float.columns] = df_float
    df_optimized[df_obj.columns] = df_obj

    print(mem_usage(df))
    print(mem_usage(df_optimized))

    md_opt = estimate_memory(df_optimized, datafile)
    write_file(md_opt, "out/3/memusage_optimized.json")

    opt_dtypes = df_optimized.dtypes
    nc = {}

    for key in selected_columns:
        nc[key] = opt_dtypes[key]

    write_file(nc, types_file)
    read_by_columns = read_csv(datafile, usecols=lambda x: x in selected_columns, dtype=nc)

    print(read_by_columns.shape)
    print("By columns size = ", mem_usage(read_by_columns))

    read_by_columns.to_csv(opt_datafile_name)

    chunks_read(datafile, selected_columns, nc, chunk_filename)


if __name__ == "__main__":

    get_stat_and_optimize(filename)

    need_dtypes = read_pandas_types(types_file)

    print(need_dtypes)

    df_plot = read_csv(opt_datafile_name, usecols=lambda x: x in need_dtypes.keys(), dtype=need_dtypes)

    df_plot.info(memory_usage='deep')

    #1
    sns.pairplot(df_plot).savefig("out/3/pairplot.png")

    #2
    plt.figure(figsize=(16,6))
    df_del = df_plot.groupby(['MONTH'])[['AIRLINE_DELAY', 'WEATHER_DELAY']].agg({ 'AIRLINE_DELAY' : ['max'],
                                                                                   'WEATHER_DELAY' : ['max']})
    plot = df_del.plot(title="delay per month")
    plot.get_figure().savefig("out/3/delay.png")

    #3
    fig, ax = plt.subplots()

    plt.figure(figsize=(30,5))
    sns.set_style("ticks",{'axes.grid' : True})
    sns.lineplot(data=df_plot, x="DISTANCE", y="AIR_TIME").set(title='Distance and air time')
    plt.savefig("out/3/airtime_by_dist.png")

    #4
    plot2 = df_plot['DESTINATION_AIRPORT'].head(20).value_counts().plot(kind='pie', title='Destination airport', autopct='%1.0f%%')
    plot2.get_figure().savefig("out/3/pie_destination.png")

    #5
    df_m = df_plot.groupby(['MONTH'])[['DISTANCE']].agg({'DISTANCE': ['sum']})

    plot2 = df_m.plot(kind='barh', title="Distance per month", figsize=(30,15))
    plot2.get_figure().savefig("out/3/distance_per_month.png")
