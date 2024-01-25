from pandas import read_csv, set_option
import seaborn as sns
import matplotlib.pyplot as plt
from common import estimate_memory, write_file, convert_object_datatypes, int_downcast, float_downcast, mem_usage, chunks_read, read_pandas_types

set_option("display.max_rows", 20, "display.max_columns", 60)

filename = "data/4/vacancies.csv"

types_file = "out/4/df_types.json"
opt_datafile_name = "data/4/vacancies_optcols.csv"
chunk_filename = "data/4/vacancies_df_chunk.csv"

selected_columns = ['id', 'schedule_id', 'premium', 'employer_id', 'type_id', 'type_name',
                    'salary_from', 'salary_to', 'code', 'employment_id']


def get_stat_and_optimize(datafile, chzs=None):
    md = {}
    md_opt = {}
    conv_d_obj = {}
    conv_d_int = {}
    conv_d_float = {}
    total_mem_usage = 0
    total_optimal_memusage = 0

    for df_chunk in read_csv(datafile, chunksize=chzs):
        print("read chunk")
        md = estimate_memory(df_chunk, datafile, md)
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

    print(f"Mem used {total_mem_usage} Optimal mem used {total_optimal_memusage}")

    write_file(conv_d_obj, "out/4/objects_memopt.json")
    write_file(conv_d_int, "out/4/int_downcast.json")
    write_file(conv_d_float, "out/4/float_downcast.json")

    write_file(md, "out/4/memusage_noopt.json")
    write_file(md_opt, "out/4/memusage_optimized.json")

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
    get_stat_and_optimize(filename, chzs=200_000)

    need_dtypes = read_pandas_types(types_file)
    print(need_dtypes)

    df_plot = read_csv(opt_datafile_name, usecols=lambda x: x in need_dtypes.keys(), dtype=need_dtypes)

    df_plot.info(memory_usage='deep')

    #1
    sns.pairplot(df_plot).savefig("out/4/pairplot.png")

    #2
    df_m = df_plot.groupby(['premium'])[['salary_from', 'salary_to']].agg(
        {
            'salary_from': ['min', 'mean', 'max'],
            'salary_to': ['min', 'mean', 'max'],
        }
    )

    plot2 = df_m.plot(kind='barh', title="premium sale", rot=90, figsize=(30,15))
    plot2.get_figure().savefig("out/4/premium.png")

    #3
    fig, ax = plt.subplots()
    sns.jointplot(x="employer_id", y="salary_from", data=df_plot, hue="premium")
    plt.savefig("out/4/employer_salary.png")

    #4
    plot2 = df_plot['schedule_id'].value_counts().plot(kind='pie', title='schedule', autopct='%1.0f%%')
    plot2.get_figure().savefig("out/4/pie_schedule.png")

    #5
    df_sched = df_plot.groupby(['schedule_id', 'premium'])[['salary_from']].agg({'salary_from': ['mean']})
    print(df_sched.head(10))

    plot2 = df_sched.plot(kind='barh', title="salary_by_sched", figsize=(30,15))
    plot2.get_figure().savefig("out/4/salary_by_schedule.png")
