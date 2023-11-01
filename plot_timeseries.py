# plot timeseries from the given csv file using matplotlib

import matplotlib.pyplot as plt
import pandas as pd
import json
from datetime import datetime, timedelta
from matplotlib.backends.backend_pdf import PdfPages


def get_node_group_utilization_details(nodes_list, cpuMap, memMap, appsMap, time):
    
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


def get_node_group_details(nodes_list, cpuMap, memMap, appsMap, cpuCapacityMap, memoryCapacityMap, time):
    
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
        if nodeGroupName not in cpuCapacityMap.keys():
            cpuCapacityMap[nodeGroupName] = {}
        if nodeGroupName not in memoryCapacityMap.keys():
            memoryCapacityMap[nodeGroupName] = {}    
    
        cpuMap[nodeGroupName][time] = cpuOfWorkloads
        memMap[nodeGroupName][time] = memoryOfWorkloads
        appsMap[nodeGroupName][time] = len(appsWithResourceDetails) 
        cpuCapacityMap[nodeGroupName][time] = totalCpuCapacity
        memoryCapacityMap[nodeGroupName][time] = totalMemoryCapacity       


# plot time series
def plot_utilization_timeseries():
    df = pd.read_csv('sedai_cluster_optimization_projections.csv')
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
            get_node_group_utilization_details(details, cpuMap, memMap, appsMap, time)  

        plot_graph(cpuMap, account, 'CPU Timeseries Data', 'CPU Utilization Percentage')    
        plot_graph(memMap, account, 'Memory Timeseries Data', 'Memory Utilization Percentage')    

        #plot_graph_per_nodeGroup(cpuMap, memMap, appsMap, account, 'Timeseries Data')  

        break    # we need dev cluster info only for now


def plot_capacity_timeseries(daysForChart=30):
    df = pd.read_csv('sedai_cluster_optimization_projections.csv')
    account_list = df['account_id'].unique()

    for account in account_list:
        account_df = df[df['account_id'] == account]
        # Get current date
        current_date = datetime.now()

        # Calculate the date 30 days ago
        thirty_days_ago = current_date - timedelta(days=daysForChart)

        account_df['updated_time'] = pd.to_datetime(account_df['updated_time'])
        
        # Remove records from a specific date (e.g., October 26th, 2023)
        specific_date = datetime(2023, 10, 26)
        specific_date = pd.to_datetime(specific_date)

        thirty_days_ago = pd.Timestamp(current_date - pd.Timedelta(days=30), tz='UTC')
        current_date = pd.Timestamp(current_date, tz='UTC')

        account_df = account_df[account_df['updated_time'].dt.date != specific_date.date()]
        account_df = account_df[(account_df['updated_time'] >= thirty_days_ago) & (account_df['updated_time'] <= current_date)]

        acc_details = account_df['details']
        timeseries = account_df['updated_time']
        dict_data = acc_details.to_dict()

        nodeGroupsJson = [json.loads(item) for item in dict_data.values()]
        nodeGroups = { time: item['optimizationRecommendations']['current']['nodeGroups'] for time,item in zip(timeseries, nodeGroupsJson) }

        cpuMap = {}
        memMap = {}
        appsMap = {}
        cpuCapacityMap = {}
        memCapacityMap = {}
        for time, details in nodeGroups.items():           
            get_node_group_details(details, cpuMap, memMap, appsMap, cpuCapacityMap, memCapacityMap, time)  
   
        pdf_pages = PdfPages('time_series_plots.pdf')
        plot_nodegroup_capacity_vs_utilization(pdf_pages, cpuMap, cpuCapacityMap, account, 'CPU Timeseries Data', 'Cores') 
        plot_nodegroup_capacity_vs_utilization(pdf_pages, memMap, memCapacityMap, account, 'Memory Timeseries Data', 'GibiBytes') 
        pdf_pages.close()

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


def plot_graph_per_nodeGroup(cpu_map, memory_map, apps_map, account, title):
    # Create a figure for each node group  

    for nodeGroup in cpu_map.keys():
        plt.figure(figsize=(10, 6))
        cpu_data = cpu_map[nodeGroup]
        memory_data = memory_map[nodeGroup]
        apps_data = apps_map[nodeGroup]
        dates = [datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f %z') for date_str in cpu_data.keys()]
        cpu_values = list(cpu_data.values())
        mem_values = list(memory_data.values())
        app_values = list(apps_data.values())

        # Filter data for dates starting from Oct 1
        oct_1_index = next((i for i, date in enumerate(dates) if date.day >= 1 and date.month >= 10), None)
        if oct_1_index is not None:
            dates = dates[oct_1_index:]
            cpu_values = cpu_values[oct_1_index:]
            mem_values = mem_values[oct_1_index:]
            app_values = app_values[oct_1_index:]

            # Plotting
            plt.plot(dates, cpu_values, marker='o', label='CPU')
            plt.plot(dates, mem_values, marker='o', label='MEMORY')
            plt.plot(dates, app_values, marker='o', label='APPS')

        plt.xlabel('Time')
        plt.ylabel('Utilization percentage')
        plt.title(title + " for " + nodeGroup)
        plt.xticks(rotation=20)
        plt.tight_layout()
        plt.legend()

    plt.show()    


    # bring a chart to show nodegroup capacity vs node utilization
def plot_capacity_vs_utilization(utilization_map, capacity_map, account, title, unit):
    # Create a single figure
    plt.figure(figsize=(10, 6))

    for nodeGroup in utilization_map.keys():
        util_data = utilization_map[nodeGroup]
        capacity_data = capacity_map[nodeGroup]
        dates = [datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f %z') for date_str in util_data.keys()]
        util_values = list(util_data.values())
        capacity_values = list(capacity_data.values())

        # Filter data for dates starting from Oct 1
        oct_1_index = next((i for i, date in enumerate(dates) if date.day >= 1 and date.month >= 10), None)
        if oct_1_index is not None:
            dates = dates[oct_1_index:]
            util_values = util_values[oct_1_index:]
            capacity_values = capacity_values[oct_1_index:]

            # Plotting
            plt.plot(dates, util_values, marker='o', label=str(account + "_" + nodeGroup))
            plt.plot(dates, capacity_values, marker='o', label=str(account + "_" + nodeGroup + "_capacity"))

    plt.xlabel('Time')
    plt.ylabel(unit)
    plt.title(title)
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.legend()

    plt.show()

def plot_nodegroup_capacity_vs_utilization(pdf_pages, utilization_map, capacity_map, account, title, unit):

    for nodeGroup in utilization_map.keys():
        plt.figure(figsize=(10, 6))
        util_data = utilization_map[nodeGroup]
        capacity_data = capacity_map[nodeGroup]
        dates = [date_str.to_pydatetime() for date_str in util_data.keys()]
        util_values = list(util_data.values())
        capacity_values = list(capacity_data.values())

        plt.stackplot(dates, util_values, capacity_values, labels=['Utilization', 'Capacity'])
              
        # TODO Take the correct date from DB
        target_date = datetime(2023, 10, 15)

        # Add a vertical dotted line at the target date
        plt.axvline(x=target_date, color='r', linestyle='--', label='Optimization Started Date')

        plt.xlabel('Time')
        plt.ylabel(unit)
        plt.title(title + " - " + nodeGroup)
        plt.xticks(rotation=20)
        plt.tight_layout()
        plt.legend(loc='upper right')  

        # Add the plot to the PDF
        pdf_pages.savefig()

    plt.show()    


#plot_utilization_timeseries()
plot_capacity_timeseries()