# plot timeseries from the given csv file using matplotlib

import matplotlib.pyplot as plt
import pandas as pd
import json
from datetime import datetime


def get_node_group_details(nodes_list, cpuMap, memMap, appsMap, time):
    
    for nodeGroup in nodes_list:

        nodeGroupName = nodeGroup["nodeGroupName"]
        instanceType = nodeGroup["instanceType"]
        totalMemoryCapacity = nodeGroup["memoryInBytes"] * nodeGroup["numberOfNodes"]
        totalCpuCapacity = nodeGroup["vcpus"] * nodeGroup["numberOfNodes"]

        appsWithResourceDetails = nodeGroup["appsWithResourceDetails"]
        
        memoryOfWorkloads = 0
        cpuOfWorkloads = 0
        for workload in appsWithResourceDetails:
            memoryOfWorkloads += workload["appTotalMemory"]
            cpuOfWorkloads += workload["appTotalCPU"]

        print("Node Group Name: ", nodeGroupName)
        print("Instance Type: ", instanceType)
        print("Total Memory Capacity: ", totalMemoryCapacity)
        print("Memory of Workloads: ", memoryOfWorkloads)    
        print("Total CPU Capacity", totalCpuCapacity)
        print("CPU of Workloads: ", cpuOfWorkloads)
        print("Number of nodes:", nodeGroup["numberOfNodes"])
        print("Number of Apps:", len(appsWithResourceDetails))

        if nodeGroupName not in cpuMap.keys():
            cpuMap[nodeGroupName] = {}
        if nodeGroupName not in memMap.keys():
            memMap[nodeGroupName] = {}
        if nodeGroupName not in appsMap.keys():
            appsMap[nodeGroupName] = {}
    
        cpuMap[nodeGroupName][time] = (cpuOfWorkloads/totalCpuCapacity)*100
        memMap[nodeGroupName][time] = (memoryOfWorkloads/totalMemoryCapacity)*100
        appsMap[nodeGroupName][time] = len(appsWithResourceDetails)


# plot time series
def plot_timeseries():

    
    df = pd.read_csv('sedai_cluster_optimization_projections.csv')
    print(df.columns)

    
    account_list = df['account_id'].unique()

    for account in account_list:
        account_df = df[df['account_id'] == account]
        acc_details = account_df['details']
        timeseries = account_df['updated_time']
        dict_data = acc_details.to_dict()

        nodeGroupsJson = [json.loads(item) for item in dict_data.values()]
        nodeGroups = { time: item['optimizationRecommendations']['current']['nodeGroups'] for time,item in zip(timeseries, nodeGroupsJson) }

        cpuMap = {}
        memMap = {}
        appsMap = {}
        for time, details in nodeGroups.items():           
            get_node_group_details(details, cpuMap, memMap, appsMap, time)  

        plot_graph(cpuMap, account, 'CPU Timeseries Data', 'Cores')    
        plot_graph(memMap, account, 'Memory Timeseries Data', 'Bytes')          

        break    # we need dev cluster info only for now

    
def plot_graph(data_map, account, title, unit):
    # Create a single figure
    plt.figure(figsize=(10, 6))

    for nodeGroup in data_map.keys():
        data = data_map[nodeGroup]
        dates = [datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f %z') for date_str in data.keys()]
        values = list(data.values())

        # Filter data for dates starting from Oct 1
        oct_1_index = next((i for i, date in enumerate(dates) if date.day >= 1 and date.month >= 10), None)
        if oct_1_index is not None:
            dates = dates[oct_1_index:]
            values = values[oct_1_index:]

            # Plotting
            plt.plot(dates, values, marker='o', label=str(account + "_" + nodeGroup))

    plt.xlabel('Time')
    plt.ylabel(unit)
    plt.title(title)
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.legend()

    plt.show()


plot_timeseries()