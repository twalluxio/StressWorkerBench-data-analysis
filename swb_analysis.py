import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# Parse from raw JSON result.
# SWB Test Summary: https://docs.google.com/spreadsheets/d/1oe3WrOkSS8HjN78LzgVTIvq8kugQDjDuWWBx9aAGcYo/
def json_to_csv(file_name):
    with open(file_name, 'r') as fr:
        json_data = json.load(fr)
        # print(json_data['results']['iombps'])
        node_results = json_data['results']['nodeResults']
        throughput_percentiles = json_data['results']['throughputPercentiles']
        for i in range(101):
            throughput_percentiles[i] = int(throughput_percentiles[i])
        with open(file_name.replace(".json", ".csv"), 'w') as fp:
            print("Worker", end="", file=fp)
            for i in range(101):
                print(',' + str(i), end="", file=fp)
            print("", file=fp)
        for worker in node_results.keys():
            with open("./" + file_name.replace("json/", "csv/").replace(".json", ".csv") + "", 'a') as fp:
                print(worker, end="", file=fp)
                for percentile in node_results[worker]['throughputPercentiles']:
                    print(',' + str(int(percentile)), end="", file=fp)
                print("", file=fp)
        return throughput_percentiles


# Set and draw the distribution graph. Also print percentiles.
# Results are shown in the command line; can directly paste into the SWB Test Summary
# Normally no need to change. Refer to matplotlib.plot if changes are necessary.
def generate_distribution(p, file):
    # generate canvas
    plt.figure(dpi=300, figsize=(8, 4.5))

    # read data
    data = pd.read_csv(file.replace(".json", ".csv"))

    # red line: emphasize on network bandwidth at 200mbps/thread if necessary
    # plt.axhline(200, c='r', lw=1)

    # draw each worker's distribution
    percentile_ids = range(100, -1, -1)
    for index, row in data.iterrows():
        throughputs = row[1:]
        plt.plot(percentile_ids, throughputs, label=row[0].replace("StressWorkerBenchCluster-", ""),
                 linewidth=1, marker='.', markersize=2)

    # draw overall distribution
    plt.plot(percentile_ids, p, label="all workers", linewidth=1.5, marker='.', markersize=4)

    # figure properties
    plt.title("Throughput of " + ''.join(file.split('/')[1:]).replace(".json", ""))
    plt.xlabel("Percentile")
    plt.xticks(np.arange(0, 101, step=10))
    plt.xlim(-5, 106)
    plt.ylabel("Throughput (MB/s)")
    plt.ylim(p[100] * 1.1, 0)
    plt.grid()
    plt.legend(loc='lower right', prop={'size': 'x-small'})

    # print percentiles on command line
    with open(file, 'r') as fr:
        json_data = json.load(fr)
        print(str(json_data['results']['iombps']) + '\t\t\t' + str(p[100]) + '\t' + str(p[99]) + '\t' + str(
            p[75]) + '\t' + str(p[50]) + '\t' + str(p[25]) + '\t' + str(p[1]) + '\t' + str(p[0]))

    # print percentiles as captions in the figure
    for percentile in [1, 25, 50, 75, 99]:
        plt.text(percentile_ids[percentile], p[100] * 0.05,
                 "p" + str(percentile_ids[percentile]) + "=" + str(p[percentile]),
                 ha='center', fontsize='x-small')
    plt.text(0, -p[100] * 0.05, "max=" + str(p[100]), ha='center', fontsize='x-small')
    plt.text(100, -p[100] * 0.05, "min=" + str(p[0]), ha='center', fontsize='x-small')

    # show and output
    plt.savefig(file.replace("json/", "fig/").replace(".json", ".png"))
    # plt.show()
    plt.cla()


# main function
# before running the main function, do the following steps:
# 0. understanding overmind
#    refer to: https://tachyonnexus.atlassian.net/wiki/spaces/~6389b74c2acfad92d7b4a2fc/pages/6225935/Overmind+101
# 1. config StressWorkerBench in overmind according to test requirements
# 2. run StressWorkerBench in overmind:
#    cd directory/to/overmind
#    ../godelw dist overmind
#    bin/overmind cluster StressWorkerBenchCluster StressWorkerBenchCluster deployWithDestroy
# 3. collect results from overmind:
#    bin/overmind cluster StressWorkerBenchCluster StressWorkerBenchCluster downloadInfo --buildId <build-id>
#    the JSON file at logs/<build-id>/StressWorkerBenchCluster/StressWorkerBenchCluster/0
#    flame graphs at logs/<build-id>/StressWorkerBenchCluster/StressWorkerBenchCluster/0/archives/<time>/stressbench
# 4. copy files to this directory, rename it by a pattern, then run the main functions
#    files use names "config-run"
#    for example, if we need to analyze the size of buffer size
#    the config set can be: ['256k', '512k', '1m', '2m', '4m']
#    then the files are like: ['256k-local.json', '256k-remote.json', ... , '512k-local.json', ...]
if __name__ == '__main__':
    # set directory
    set_id = "231113_1"

    # configs to run, one config run in a cluster
    config_set = ['set1', 'set2', 'set3']

    # One config set runs 4 times, testing local/remote cross sequential/random.
    # You can remove some of them if not run
    run_set = ['local', 'localRandom', 'remote', 'remoteRandom']

    for i in config_set:
        for j in run_set:
            filename = set_id + "/" + i + "-" + j + ".json"
            percentiles = json_to_csv(filename)
            generate_distribution(percentiles, filename)
            os.remove(filename.replace(".json", ".csv"))
