import os
import sys

algorithms = [
    'pandas',
    'datatable', 
    'polars', 
    'modin_ray', 
    'modin_dask',
    'pyspark_pandas', 
    'spark', 
    'vaex'
]

# parts = ['1', '2', '3', '4']

# algorithms = algorithms[:2]
# parts = parts[:4]

save_output_results = False
max_num_tests = 2
modes = [''] #, '--pipeline-step', '--pipeline']   # the first is for core execution (default)

total_nruns = len(algorithms) * len(modes) * max_num_tests
run_cnt = 0

if input(f'Total number of runs to do: {total_nruns}, continue? (y/n) ') != 'y':
    sys.exit()


for mode in modes:
    for algorithm in algorithms:
        for i in range(max_num_tests):
            perc = run_cnt / total_nruns * 100
            print(f'==================== algorithm {algorithm} - mode {mode if mode else "core"} - test {i + 1} - completed {format(perc, ".2f")}% ===============================')
            os.system(f'python run_algorithm.py --algorithm {algorithm} --dataset crypto-market --locally {mode}')

            run_cnt += 1

            if save_output_results:
                if algorithm == 'modin_ray':
                    os.rename('pipeline_output/modin_loan_output.csv', 'pipeline_output/modin_ray_output.csv')
                if os.path.exists(f'pipeline_output/q{part}'):
                    os.system(f'rm -rf pipeline_output/q{part}')

                os.mkdir(f'pipeline_output/q{part}')
                os.system(f'mv pipeline_output/*.csv pipeline_output/q{part}')
